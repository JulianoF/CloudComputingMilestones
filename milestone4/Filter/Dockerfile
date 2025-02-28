FROM python:3.9
RUN pip install google-cloud-pubsub
COPY *.json .
ADD filter.py .
CMD ["python","-u", "./filter.py"] 