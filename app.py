# Complete modernized code for app.py

# ... (rest of your modernized app.py code) ...

# This is where we remove the empty premium-card div and add a horizontal separator line.
# Updated Section

# Daily Receipt Portal

# horizontal separator
import io
import flask
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def homepage():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
