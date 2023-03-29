FROM python:3.11.2
WORKDIR /app
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . ./
CMD ["python", "-u", "compare_indexes_experiment/client.py"]