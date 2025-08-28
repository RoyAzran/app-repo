FROM python:3.9

WORKDIR /app

COPY . .
RUN echo "Flask==3.0.0" > requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000

CMD ["python", "api.py"]
CMD ["python3","-m", "unittest" ,"discover", "-s", "tests" , "-v"]
