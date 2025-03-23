# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Expose any necessary ports (optional, Kafka and MySQL will be handled by Docker Compose)
EXPOSE 9092 3306

# Run the application
CMD ["python", "main_producer.py"]  # Default command for producer