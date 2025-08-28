FROM python:3.9

WORKDIR  /calculator-app-v2

COPY . .
RUN pip install --no-cache-dir -r /requirements.txt

EXPOSE 5000

CMD ["python3", "api.py"]
CMD ["python3 , "-m" , "unittest" , "discover" , "-s" ,"tests" ,"-v"]
