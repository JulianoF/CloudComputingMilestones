FROM python:3.9
RUN pip install google-cloud-pubsub
COPY *.json .
ADD convert.py .
CMD ["python","-u", "./convert.py"] 