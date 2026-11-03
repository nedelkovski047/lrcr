# LRCR on Render (Docker runtime). Docker is used because Playwright's Chromium needs system
# libraries that Render's native Python runtime does not ship.
#
# The tag below MUST equal the `playwright==` pin in requirements.txt. The build fails on purpose
# if they differ (a mismatched Playwright cannot find the browsers baked into this image).
ARG PW_VERSION=1.63.0
FROM mcr.microsoft.com/playwright/python:v${PW_VERSION}-noble
# ARGs declared before FROM are not visible after it; re-declare to use it in RUN.
ARG PW_VERSION

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first so Docker caches this layer until requirements.txt changes.
COPY requirements.txt ./
RUN apt-get update && apt-get install -y python3.12-venv \
 && python -m venv /opt/venv \
 && /opt/venv/bin/pip install -r requirements.txt
ENV PATH="/opt/venv/bin:$PATH"

# Fail the build if pip's Playwright differs from the image's browsers.
RUN python -c "import importlib.metadata as m, os, sys; have=m.version('playwright'); want=os.environ['PW_VERSION']; sys.exit(0 if have==want else f'playwright {have} in requirements.txt != {want} in the Dockerfile FROM tag; make them equal'); "

COPY . .

# The image ships an unprivileged user; do not run the web process as root.
RUN chown -R pwuser:pwuser /app
USER pwuser

# Render injects PORT (default 10000). Shell form so ${PORT} is expanded.
EXPOSE 10000
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --proxy-headers --forwarded-allow-ips='*'"]
