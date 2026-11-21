# Shopping Sessions reference implementation (plain Python + pyarrow).
# No FROM line pins a platform: the scorers build this natively on linux/amd64.
FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY reference.py run.sh /app/
# The scorer runs as uid 1000, not the files' owner: make them world-readable
# whatever permissions the checkout had.
RUN chmod 0755 /app /app/run.sh && chmod 0644 /app/reference.py /app/requirements.txt

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
CMD ["/app/run.sh"]
