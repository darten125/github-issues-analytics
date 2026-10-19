"""Curated GitHub Issues marts built only from a consistent DDS publication."""

RULES_VERSION = "1"
EXPECTED_REPOSITORIES = (
    "Kotlin/kotlinx.coroutines",
    "ktorio/ktor",
    "android/nowinandroid",
)


class CdmError(ValueError):
    """A safe, stable error code for CDM publication and validation."""

    def __init__(self, code: str, details: str | None = None):
        self.code = code
        super().__init__(f"{code}: {details}" if details else code)
