# Use the official Python 3.10 slim image as the base image
FROM python:3.10-slim

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install Python dependencies specified in requirements.txt without using cache
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application files from the host to the container's working directory
COPY . .

# Set the default command to run the application
CMD ["python", "app.py"]