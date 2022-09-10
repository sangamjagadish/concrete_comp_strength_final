# we're tring to get python 3.7
# we try to copy everything into app folder
# we set app folder as our working directory
# we install all the requirements mentioned in requirement file
# Now we are using main.py file to run our application
FROM python:3.7 
COPY . /app 
WORKDIR /app 
RUN pip install -r requirements.txt 
ENTRYPOINT ["python"]
CMD ["main.py"] 