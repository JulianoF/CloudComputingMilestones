# Use an official Python runtime as a parent image.
FROM python:3.9-slim

# Install system dependencies needed by OpenCV and PyTorch.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip.
RUN pip install --upgrade pip

# Copy the requirements file and install Python dependencies.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy your application code to the container.
# Assume your main script is called pipeline.py and that your YOLO model file (yolo11n.pt) is also in this directory.
WORKDIR /app
COPY DesignPipeline.py ./DesignPipeline.py
COPY MiDaS/ ./MiDaS/
COPY dpt_levit_224.pt ./dpt_levit_224.pt
COPY yolo11n.pt ./yolo11n.pt


# Set the entrypoint. Dataflow will pass pipeline options as command-line arguments.
CMD ["python", "DesignPipeline.py"]
