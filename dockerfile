FROM python:3.9

WORKDIR /app

COPY . /app
RUN echo "Flask==3.0.0" > requirements.txt
RUN echo "Python==3.9" >> requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN python3 -m unittest discover -s tests -v

EXPOSE 5000

CMD ["python", "api.py"]
