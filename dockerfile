FROM python:3.9
COPY . .
WORKDIR  /calculator-app-v2
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python3", "api.py"]
