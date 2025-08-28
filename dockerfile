FROM python:3.9

WORKDIR /app

COPY . /app
RUN echo "flask==3.0.0" > requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python", "api.py"]
