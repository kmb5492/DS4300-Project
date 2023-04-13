from flask import Flask, render_template, request
from pymongo import MongoClient
import os

app = Flask(__name__)

# Set up the MongoDB client and database
username = os.environ['MONGO_USER']
password = os.environ['MONGO_PASSWORD']
client = MongoClient('mongodb+srv://xu_dani:OCs4tUizjUo4prIB@danielxu0.bwjsb65.mongodb.net/test',
                     connectTimeoutMS=30000, socketTimeoutMS=None, connect=False, maxPoolsize=1)
db = client['SURVEY_TEST']
collection = db['responses']


# Route for the survey page
@app.route('/', methods=['GET', 'POST'])
def survey():
    if request.method == 'POST':
        # Get the form data from the request
        question1 = request.form['question1']
        question2 = request.form['question2']
        question3 = request.form['question3']

        # Insert the survey response into the MongoDB collection
        collection.insert_one({'question1': question1, 'question2': question2, 'question3': question3})

        # Return a thank-you message
        return render_template('thank_you.html')
    else:
        # Render the survey template
        return render_template('survey.html')


if __name__ == '__main__':
    app.run(debug=True)
