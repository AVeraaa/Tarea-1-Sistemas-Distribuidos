FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download en_core_web_md
COPY . .
EXPOSE 5001
CMD ["python3", "-m", "flask", "--app", "main", "run", "--host=0.0.0.0", "--port=5001"]