"""Environment variable settings."""

import threading
import time
from os import getenv
from pathlib import Path
from typing import Any, Literal, Optional, cast

from oauthlib.oauth2.rfc6749.clients.backend_application import BackendApplicationClient
from pydantic import (
    UUID4,
    BaseModel,
    ConfigDict,
    DirectoryPath,
    Field,
    FilePath,
    HttpUrl,
    PositiveInt,
    PrivateAttr,
    StrictBool,
    StrictStr,
    field_validator,
)
from requests import Response
from requests.exceptions import RequestException
from requests_oauthlib.oauth2_session import OAuth2Session
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_exponential,
)

from helper.default_handler import create_filedir

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
_VALID_LOG_LEVELS: tuple[LogLevel, ...] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def _get_log_level() -> LogLevel:
    """Return the configured logging level, falling back to INFO when invalid."""
    level = getenv("LOG_LEVEL", "INFO").upper()
    if level not in _VALID_LOG_LEVELS:
        return "INFO"
    return cast(LogLevel, level)


def _get_influxdb_log_level() -> LogLevel:
    """Return the configured InfluxDB logging level, falling back to LOG_LEVEL."""
    level = getenv("LOG_INFLUXDB_LEVEL", getenv("LOG_LEVEL", "INFO")).upper()
    if level not in _VALID_LOG_LEVELS:
        return "INFO"
    return cast(LogLevel, level)


def _get_influxdb_logging_enabled() -> bool:
    """Return whether application log records should be written to InfluxDB."""
    configured_value = getenv("LOG_INFLUXDB_ENABLED")
    if configured_value is not None:
        return _get_bool_env("LOG_INFLUXDB_ENABLED")
    return getenv("LOG_INFLUXDB_LEVEL") is not None


def _get_bool_env(name: str, default: bool = False) -> bool:
    """Return a boolean environment value."""
    value = getenv(name)
    if value is None:
        return default
    return value.lower() in ("true", "1", "t", "yes", "y", "on")


class ConnectionSettings(BaseModel):
    """
    Configuration model for connection and concurrency settings.

    This class defines the connection parameters and threading configuration used
    throughout the application. All values can be overridden via environment variables.

    Attributes:
        max_retry (PositiveInt): Maximum number of retry attempts for failed connections.
            Default: 3 (from MAX_RETRY env var)
        conn_timeout (PositiveInt): Connection timeout in seconds.
            Default: 30 (from CONN_TIMEOUT env var)
        read_timeout_override (PositiveInt): Read timeout override value in seconds.
            Default: 60 (from READ_TIMEOUT_OVERRIDE env var)
        num_of_threads (PositiveInt): Number of threads to use for concurrent operations.
            Default: 8 (from NUM_OF_THREADS env var)

    Validation Rules:
        - max_retry: Must be at least 1
        - conn_timeout: Must be positive (> 0)
        - read_timeout_override: Must be positive (> 0)
        - num_of_threads: Must be between 1 and 100 (inclusive)

    Raises:
        ValueError: If any field fails its validation constraints

    Example:
        >>> settings = ConnectionSettings()
        >>> settings.max_retry
        3
        >>> settings = ConnectionSettings(max_retry=5, num_of_threads=16)
        >>> settings.num_of_threads
        16
    """

    max_retry: PositiveInt = Field(
        default_factory=lambda: int(getenv("MAX_RETRY", "3")),
        description="Maximum number of retry attempts for failed connections",
    )
    conn_timeout: PositiveInt = Field(
        default_factory=lambda: int(getenv("CONN_TIMEOUT", "30")),
        description="Connection timeout in seconds",
    )
    read_timeout_override: PositiveInt = Field(
        default_factory=lambda: int(getenv("READ_TIMEOUT_OVERRIDE", "60")),
        description="Read timeout override value in seconds",
    )
    num_of_threads: PositiveInt = Field(
        default_factory=lambda: int(getenv("NUM_OF_THREADS", "8")),
        description="Number of threads to use for concurrent operations",
    )

    @field_validator("conn_timeout", "read_timeout_override")
    @classmethod
    def validate_positive_timeout(cls, v: int) -> int:
        """Validation classmethod."""
        if v <= 0:
            raise ValueError("Timeout values must be positive")
        return v

    @field_validator("max_retry")
    @classmethod
    def validate_retry_count(cls, v: int) -> int:
        """Validation classmethod."""
        if v < 1:
            raise ValueError("max_retry must be at least 1")
        return v

    @field_validator("num_of_threads")
    @classmethod
    def validate_thread_count(cls, v: int) -> int:
        """Validation classmethod."""
        if not 1 <= v <= 100:
            raise ValueError("num_of_threads must be between 1 and 100")
        return v


class LoggingSettings(BaseModel):
    """
    Configuration model for application logging settings.

    This class defines the logging configuration parameters used throughout the application,
    with values loaded from environment variables or sensible defaults.

    Attributes:
        log_file_path (Path): The file path where log files will be stored.
            Defaults to "./mandiri-MONA.log" if LOG_FILE_PATH environment variable is not set.
        log_rotate_time (str): The time interval for log rotation.
            Defaults to "w0" (weekly rotation on Monday) if LOG_ROTATE_TIME environment variable is not set.
            Common values: 'D' (daily), 'W0-W6' (weekly on specific day), 'midnight' (daily at midnight).
        log_backup_count (int): The number of backup log files to retain.
            Defaults to 9 if LOG_BACKUP_COUNT environment variable is not set.
        log_format (str): The format string for log messages.
            Defaults to "%(asctime)s - %(levelname)s - %(message)s".
        log_datetime_format (str): The datetime format string for log messages.
            Defaults to "%Y-%m-%d %H:%M:%S" if LOG_DATETIME_FORMAT environment variable is not set.
        log_influxdb_enabled (StrictBool): Flag to write log records to InfluxDB. Defaults
            to True when LOG_INFLUXDB_LEVEL is configured, unless LOG_INFLUXDB_ENABLED is set.
        log_influxdb_level (LogLevel): Minimum log level for writing log records to InfluxDB.
            Defaults to LOG_LEVEL or INFO if LOG_INFLUXDB_LEVEL environment variable is not set.
        log_influxdb_measurement (StrictStr): InfluxDB measurement name for log records.
            Defaults to "afira_logs" if LOG_INFLUXDB_MEASUREMENT environment variable is not set.

    Environment Variables:
        LOG_FILE_PATH: Path to the log file (optional)
        LOG_ROTATE_TIME: Log rotation interval (optional)
        LOG_BACKUP_COUNT: Number of backup logs to keep (optional)
        LOG_FORMAT: Log message format (optional)
        LOG_DATETIME_FORMAT: Log datetime format (optional)
        LOG_INFLUXDB_ENABLED: Enables InfluxDB log writes (optional)
        LOG_INFLUXDB_LEVEL: Minimum level for InfluxDB log writes (optional)
        LOG_INFLUXDB_MEASUREMENT: InfluxDB measurement name for log writes (optional)

    Example:
        >>> settings = LoggingSettings()
        >>> print(settings.log_file_path)
        ./logs/mandiri-MONA.log
    """

    log_file_path: Path = Field(
        default_factory=lambda: Path(getenv("LOG_FILE_PATH", "./logs/mandiri-MONA.log")).absolute(),
        description="Path to the log file",
        validate_default=True,
    )
    log_rotate_time: str = Field(
        default_factory=lambda: str(getenv("LOG_ROTATE_TIME", "w0")),
        description="Log rotation interval",
    )
    log_backup_count: int = Field(
        default_factory=lambda: int(getenv("LOG_BACKUP_COUNT", "9")),
        description="Number of backup log files to retain",
    )
    log_format: str = Field(
        default_factory=lambda: str(getenv("LOG_FORMAT", "%(asctime)s - %(levelname)s - %(message)s")),
        description="Log message format",
    )
    log_datetime_format: str = Field(
        default_factory=lambda: str(getenv("LOG_DATETIME_FORMAT", "%Y-%m-%d %H:%M:%S")),
        description="Log datetime format",
    )
    log_influxdb_enabled: StrictBool = Field(
        default_factory=_get_influxdb_logging_enabled,
        description="Enables writing application logs to InfluxDB",
    )
    log_influxdb_level: LogLevel = Field(
        default_factory=_get_influxdb_log_level,
        description="Minimum log level for InfluxDB log writes",
    )
    log_influxdb_measurement: StrictStr = Field(
        default_factory=lambda: str(getenv("LOG_INFLUXDB_MEASUREMENT", "afira_logs")),
        description="InfluxDB measurement for application logs",
    )

    @field_validator("log_file_path", mode="before")
    @classmethod
    def validate_log_file_path(cls, v: Path | str) -> Path:
        """Validate that the log file path points to a valid `.log` file and create the file if it does not exist."""
        path = Path(v) if isinstance(v, str) else v
        if path.suffix != ".log":
            raise ValueError("Log file path must point to a valid `.log` file.")
        if not path.exists():
            path = create_filedir(path, "file")
        return path


class FilePathConfig(BaseModel):
    """
    Configuration model for managing file and directory paths in the application.

    This class validates and manages paths for firewall credentials, SSH daemon configuration,
    and output directory. It automatically creates missing files and directories during validation.

    Attributes:
        fw_creds (FilePath): Path to the firewall credentials CSV file.
            Defaults to './configs/fw_creds.csv' or the value of FW_CREDS_PATH environment variable.
        sshd_config (FilePath): Path to the SSH daemon configuration file.
            Defaults to './configs/sshd_config' or the value of SSHD_CONFIG_PATH environment variable.
        output_dir (DirectoryPath): Path to the output directory.
            Defaults to './outputs/' or the value of OUTPUT_DIR_PATH environment variable.

    Notes:
        - All paths are validated before assignment
        - Missing files and directories are automatically created during validation
        - Environment variables take precedence over default values
    """

    fw_creds: FilePath = Field(
        default_factory=lambda: Path(getenv("FW_CREDS_PATH", "./configs/fw_creds.csv")).absolute(),
        description="Path to firewall credentials CSV file.",
    )
    sshd_config: FilePath = Field(
        default_factory=lambda: Path(getenv("SSHD_CONFIG_PATH", "./configs/sshd_config")).absolute(),
        description="Path to SSH daemon configuration file.",
    )
    output_dir: DirectoryPath = Field(
        default_factory=lambda: Path(getenv("OUTPUT_DIR_PATH", "./outputs/")).absolute(),
        description="Path to output directory.",
    )

    @field_validator("fw_creds", "sshd_config", mode="before")
    @classmethod
    def validate_file_paths(cls, v: Path | str) -> Path:
        """Validate that the file paths point to valid files and create the files if they do not exist."""
        path = Path(v) if isinstance(v, str) else v
        if not path.exists():
            path = create_filedir(path, "file")
        return path

    @field_validator("output_dir", mode="before")
    @classmethod
    def validate_output_dir(cls, v: Path | str) -> Path:
        """Validate that the output directory path points to a valid directory and create it if it does not exist."""
        path = Path(v) if isinstance(v, str) else v
        if not path.exists():
            path = create_filedir(path, "dir")
        return path


class InfluxDBSettings(BaseModel):
    """
    InfluxDBSettings defines configuration parameters for connecting to an InfluxDB instance.

    Attributes:
        token (StrictStr): InfluxDB authentication token, loaded from environment variable 'INFLUXDB_TOKEN'.
        url (StrictStr): InfluxDB URL, loaded from environment variable 'INFLUXDB_URL'.
        org (StrictStr): InfluxDB organization, loaded from environment variable 'INFLUXDB_ORG'.
        bucket (StrictStr): InfluxDB bucket, loaded from environment variable 'INFLUXDB_BUCKET'.
        connection_pool_maxsize (PositiveInt): Maximum number of concurrent HTTP connections to InfluxDB.
        max_retries (PositiveInt): Maximum retry attempts for failed writes.
        timeout_ms (PositiveInt): Request timeout in milliseconds.
        batch_size (PositiveInt): Number of points to batch before writing.
        flush_interval_ms (PositiveInt): Flush interval in milliseconds.

    Methods:
        conn_params() -> dict[str, str | int]: Returns a dictionary containing all connection parameters.
    """  # noqa: E501

    token: StrictStr = Field(
        default_factory=lambda: (getenv("INFLUXDB_TOKEN") or getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN") or ""),
        description="InfluxDB authentication token",
    )
    url: StrictStr = Field(default_factory=lambda: getenv("INFLUXDB_URL", ""), description="InfluxDB URL")
    org: StrictStr = Field(
        default_factory=lambda: (getenv("INFLUXDB_ORG", "") or getenv("DOCKER_INFLUXDB_INIT_ORG", "")),
        description="InfluxDB organization",
    )
    bucket: StrictStr = Field(
        default_factory=lambda: (getenv("INFLUXDB_BUCKET", "") or getenv("DOCKER_INFLUXDB_INIT_BUCKET", "")),
        description="InfluxDB bucket",
    )
    connection_pool_maxsize: PositiveInt = Field(
        default_factory=lambda: int(getenv("INFLUXDB_CONNECTION_POOL_MAXSIZE", "10")),
        description="Maximum number of concurrent HTTP connections to InfluxDB",
    )
    max_retries: PositiveInt = Field(
        default_factory=lambda: int(getenv("INFLUXDB_MAX_RETRIES", "2")),
        description="Maximum retry attempts for failed writes",
    )
    timeout_ms: PositiveInt = Field(
        default_factory=lambda: int(getenv("INFLUXDB_TIMEOUT_MS", "30_000")),
        description="Request timeout in milliseconds",
    )
    batch_size: PositiveInt = Field(
        default_factory=lambda: int(getenv("INFLUXDB_BATCH_SIZE", "500")),
        description="Number of points to batch before writing",
    )
    flush_interval_ms: PositiveInt = Field(
        default_factory=lambda: int(getenv("INFLUXDB_FLUSH_INTERVAL_MS", "10_000")),
        description="Flush interval in milliseconds",
    )

    def conn_params(self) -> dict[str, str | int | bool]:
        """Return InfluxDB connection parameters as a dictionary."""
        return {
            "url": str(self.url),
            "token": str(self.token),
            "org": str(self.org),
            "timeout": int(self.timeout_ms),
            "enable_gzip": True,
            "connection_pool_maxsize": int(self.connection_pool_maxsize),
        }


class EnvironmentsVariables(BaseModel):
    """
    Pydantic model for managing application environment variables and configuration settings.

    This class handles the loading and validation of environment variables from a .env file,
    providing structured access to various configuration settings including debug mode,
    logging configuration, file paths, and connection settings.

    Attributes:
        debug_mode (StrictBool): Flag to enable debug mode. Reads from DEBUG_MODE environment
            variable. Accepts "true", "1", or "t" (case-insensitive) as True values.
        log_level (Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]): The logging
            level for the application. Defaults to "INFO" if not specified.
        loop_sleep_seconds (PositiveInt): Sleep interval between AFIRA collection
            cycles. Reads from AFIRA_LOOP_SLEEP_SECONDS and defaults to 300.
        compatibility_mode (StrictBool): Flag to enable compatibility mode for older reports
            models. Defaults to False.
        verbose (StrictBool): User verbosity flag. When set to True, changes the debug
            level to DEBUG. Defaults to False.
        file_paths (FilePathConfig): Configuration object for file paths used by the
            application. Created using default factory.
        conn (ConnectionSettings): Configuration object for connection settings.
            Created using default factory.
        logging (LoggingSettings): Configuration object for logging settings.
            Created using default factory.
        influxdb (InfluxDBSettings): Configuration object for InfluxDB settings.
            Created using default factory.

    Example:
        >>> env = EnvironmentsVariables()
        >>> print(env.debug_mode)
        False
        >>> print(env.log_level)
        'INFO'

    Note:
        The .env file is expected to be located at the root of the project directory.
        Environment variables are automatically loaded during initialization.
    """

    debug_mode: StrictBool = Field(
        default_factory=lambda: _get_bool_env("DEBUG_MODE"),
        description="Enables debug mode",
    )
    log_level: LogLevel = Field(default_factory=_get_log_level, description="Logging level for the application")
    loop_sleep_seconds: PositiveInt = Field(
        default_factory=lambda: int(getenv("AFIRA_LOOP_SLEEP_SECONDS", "300")),
        description="Sleep interval in seconds between AFIRA collection cycles",
    )
    compatibility_mode: StrictBool = Field(False, description="Enables compatibility mode for older reports models")
    verbose: StrictBool = Field(False, description="User verbosity flag, will change debug level to DEBUG if set")
    file_paths: FilePathConfig = Field(default_factory=lambda: FilePathConfig())
    conn: ConnectionSettings = Field(default_factory=lambda: ConnectionSettings())
    logging: LoggingSettings = Field(default_factory=lambda: LoggingSettings())
    influxdb: InfluxDBSettings = Field(default_factory=lambda: InfluxDBSettings())

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: str | None) -> LogLevel:
        """Validate and convert log level from environment variable."""
        if v is None:
            return _get_log_level()
        level = str(v).upper()
        if level not in _VALID_LOG_LEVELS:
            raise ValueError(f"log_level must be one of {_VALID_LOG_LEVELS}")
        return cast(LogLevel, level)


class HPEOAuth2Client(BaseModel):
    """OAuth 2.0 client-credentials helper for HPE API requests.

    This model wraps a :class:`requests_oauthlib.OAuth2Session` configured with
    ``oauthlib``'s backend application client. It lazily fetches an access token,
    caches it until shortly before expiry, and reuses the same OAuth session for
    subsequent HTTP calls.

    The client is safe to share across worker threads in this application. Token
    refreshes and outgoing requests are guarded by a re-entrant lock so callers do
    not race while replacing the cached token or using the shared session.

    Attributes:
        token_url (HttpUrl): OAuth 2.0 token endpoint used to fetch client-credentials tokens.
        base_url (HttpUrl): Base URL for the HPE API.
        client_id (UUID4): OAuth 2.0 client identifier issued by HPE.
        client_secret (StrictStr): OAuth 2.0 client secret issued by HPE.
        refresh_margin_seconds (PositiveInt): Number of seconds before token expiry when the
            cached token should be refreshed. Defaults to 60 seconds.
        retry_attempts (PositiveInt): Maximum number of request attempts before returning the
            last response or raising the final transport exception. Defaults to 3.
        retry_min_seconds (PositiveInt): Minimum exponential backoff delay in seconds.
            Defaults to 1 second.
        retry_max_seconds (PositiveInt): Maximum exponential backoff delay in seconds.
            Defaults to 30 seconds.

    Private Attributes:
        _oauth: Shared OAuth session used for token fetching and HTTP requests.
        _token: Last token response returned by the token endpoint, or ``None``
            before the first successful token fetch.
        _expires_at: Unix timestamp when the cached token expires.
        _lock: Re-entrant lock protecting token refreshes and session access.

    Example:
        >>> client = HPEOAuth2Client(
        ...     token_url="https://example.com/oauth2/token",
        ...     client_id="client-id",
        ...     client_secret="client-secret",
        ... )
        >>> response = client.get("https://example.com/api/resource")
        >>> response.status_code
        200

    Note:
        Use this class as a context manager when possible so the underlying HTTP
        session is closed deterministically.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    token_url: HttpUrl
    base_url: HttpUrl
    client_id: UUID4
    client_secret: StrictStr
    refresh_margin_seconds: PositiveInt = 60
    retry_attempts: PositiveInt = 3
    retry_min_seconds: PositiveInt = 1
    retry_max_seconds: PositiveInt = 30

    _oauth: OAuth2Session = PrivateAttr()
    _token: Optional[dict[str, Any]] = PrivateAttr(default=None)
    _expires_at: float = PrivateAttr(default=0)
    _lock: threading.RLock = PrivateAttr(default_factory=threading.RLock)

    def model_post_init(self, __context: Any) -> None:
        """Create the OAuth session after Pydantic has validated public fields.

        Args:
            __context: Optional Pydantic initialization context. It is accepted
                for compatibility with ``BaseModel.model_post_init`` and is not
                used by this model.
        """
        client = BackendApplicationClient(client_id=str(self.client_id))
        self._oauth = OAuth2Session(client=client)

    def __enter__(self) -> "HPEOAuth2Client":
        """Fetch a valid token and return this client for context-manager use.

        Returns:
            The initialized OAuth client instance.
        """
        self.ensure_token()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Close the underlying OAuth session when leaving a context manager.

        Args:
            exc_type: Exception type raised inside the context, if any.
            exc: Exception instance raised inside the context, if any.
            tb: Traceback raised inside the context, if any.
        """
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP session and release pooled connections."""
        self._oauth.close()

    def ensure_token(self) -> dict[str, Any]:
        """Return a valid access token response, refreshing it when necessary.

        A new token is fetched when no token has been cached yet or when the
        cached token is within ``refresh_margin_seconds`` of its expiration time.
        The whole check-and-refresh operation is lock-protected so concurrent
        callers share a single refreshed token.

        Returns:
            The OAuth token response dictionary. It normally contains at least
            ``access_token``, ``token_type``, and ``expires_in``.
        """
        with self._lock:
            now = time.time()

            if self._token is None or now >= self._expires_at - self.refresh_margin_seconds:
                self._token = self._oauth.fetch_token(
                    token_url=str(self.token_url),
                    client_secret=self.client_secret,
                )

                expires_in = int(self._token.get("expires_in", 3600))
                self._expires_at = time.time() + expires_in

            return self._token

    @property
    def access_token(self) -> str:
        """Return the current bearer access token, refreshing it if needed.

        Returns:
            The access token string from the cached OAuth token response.
        """
        token = self.ensure_token()
        return token["access_token"]

    def _build_url(self, endpoint: str) -> str:
        """Build an absolute API URL from the configured base URL and endpoint.

        Args:
            endpoint (str): Relative API endpoint, with or without a leading slash.

        Returns:
            Absolute API URL as a plain string suitable for ``requests``.
        """
        return f"{str(self.base_url).rstrip('/')}/{endpoint.lstrip('/')}"

    @staticmethod
    def _response_status(response: Response) -> str | None:
        """Return the HPE response envelope status when the body provides one.

        HPE API responses follow the shape shown in ``example.response.json``,
        where the body contains a ``response`` object with ``status``, ``code``,
        and ``message`` fields. This method reads that status without treating
        non-JSON responses as retryable by itself.

        Args:
            response (Response): Response returned by the OAuth session.

        Returns:
            Uppercase HPE response status, or ``None`` when the field is absent.
        """
        try:
            payload = response.json()
        except ValueError:
            return None

        if not isinstance(payload, dict):
            return None

        metadata = payload.get("response")
        if not isinstance(metadata, dict):
            return None

        status = metadata.get("status")
        return str(status).upper() if status is not None else None

    @classmethod
    def _should_retry_response(cls, response: Response) -> bool:
        """Return whether an HPE API response should be retried.

        Args:
            response (Response): Response returned by the OAuth session.

        Returns:
            ``True`` when the response is rate-limited, server-side, or carries
            an HPE response envelope whose status is not ``SUCCESS``.
        """
        status_code = response.status_code
        if isinstance(status_code, int) and (status_code in {408, 409, 425, 429} or status_code >= 500):
            return True

        hpe_status = cls._response_status(response)
        return hpe_status is not None and hpe_status != "SUCCESS"

    @staticmethod
    def _last_retry_result(retry_state: RetryCallState) -> Response:
        """Return the final response after all response-based retries fail.

        Args:
            retry_state (RetryCallState): Tenacity state for the exhausted retry loop.

        Returns:
            Final response produced by the retried request call.

        Raises:
            RuntimeError: If Tenacity reaches this callback without a stored outcome.
        """
        outcome = retry_state.outcome
        if outcome is None:
            raise RuntimeError("Tenacity retry loop ended without a stored outcome")

        return outcome.result()

    def _retrying(self) -> Retrying:
        """Build the Tenacity retry controller for authenticated requests.

        Returns:
            Configured Tenacity ``Retrying`` instance using exponential backoff.
        """
        # BUG: Retrying seems not working properly because there is no Log and the production side seems fail at first
        # fetch error without retrying, need to investigate more
        return Retrying(
            retry=retry_if_exception_type(RequestException) | retry_if_result(self._should_retry_response),
            stop=stop_after_attempt(self.retry_attempts),
            wait=wait_exponential(min=self.retry_min_seconds, max=self.retry_max_seconds),
            retry_error_callback=self._last_retry_result,
            reraise=True,
        )

    def _send_request(self, method: Literal["GET", "POST", "PUT", "DELETE"], url: str, **kwargs) -> Response:
        """Send one authenticated request attempt through the OAuth session.

        Args:
            method (Literal["GET", "POST", "PUT", "DELETE"]): HTTP method to send.
            url (str): Absolute URL for the target resource.
            **kwargs: Additional keyword arguments forwarded to the OAuth session.

        Returns:
            Response returned by the underlying OAuth session.
        """
        with self._lock:
            return self._oauth.request(method, url, **kwargs)

    def request(self, method: Literal["GET", "POST", "PUT", "DELETE"], url: str | HttpUrl, **kwargs) -> Response:
        """Send an authenticated HTTP request through the OAuth session.

        Args:
            method (Literal["GET", "POST", "PUT", "DELETE"]): HTTP method to send, such as \
                ``"GET"``, ``"POST"``,``"PUT"``, or ``"DELETE"``.
            url (str | HttpUrl): Absolute URL for the target resource.
            **kwargs: Additional keyword arguments forwarded to
                :meth:`requests_oauthlib.OAuth2Session.request`.

        Returns:
            The first successful response, or the last retryable response after
            all retry attempts are exhausted.
        """
        self.ensure_token()
        return self._retrying()(self._send_request, method, str(url), **kwargs)

    def get(self, endpoint: str, **kwargs) -> Response:
        """Send an authenticated ``GET`` request.

        Args:
            endpoint (str): The API endpoint for the target resource.
            **kwargs: Additional request options forwarded to :meth:`request`.

        Returns:
            The response object returned by the underlying OAuth session.
        """
        return self.request("GET", self._build_url(endpoint), **kwargs)

    def post(self, endpoint: str, **kwargs) -> Response:
        """Send an authenticated ``POST`` request.

        Args:
            endpoint (str): The API endpoint for the target resource.
            **kwargs: Additional request options forwarded to :meth:`request`.

        Returns:
            The response object returned by the underlying OAuth session.
        """
        return self.request("POST", self._build_url(endpoint), **kwargs)

    def put(self, endpoint: str, **kwargs) -> Response:
        """Send an authenticated ``PUT`` request.

        Args:
            endpoint (str): The API endpoint for the target resource.
            **kwargs: Additional request options forwarded to :meth:`request`.

        Returns:
            The response object returned by the underlying OAuth session.
        """
        return self.request("PUT", self._build_url(endpoint), **kwargs)

    def delete(self, endpoint: str, **kwargs) -> Response:
        """Send an authenticated ``DELETE`` request.

        Args:
            endpoint (str): The API endpoint for the target resource.
            **kwargs: Additional request options forwarded to :meth:`request`.

        Returns:
            The response object returned by the underlying OAuth session.
        """
        return self.request("DELETE", self._build_url(endpoint), **kwargs)
