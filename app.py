import functools
import sys
import click
from flask import (
    Flask, current_app, flash, g, redirect, render_template, request, session, url_for
)
from flask_login import current_user, LoginManager
from werkzeug.security import check_password_hash, generate_password_hash
import pickle
import datetime
import sqlite3
import csv
import pandas as pd
import numpy as np

app = Flask(__name__)

app.secret_key = 'your_secret_key_here'


conn = sqlite3.connect('library.db')
c = conn.cursor()

# Creating the Students and Books tables if they do not already exist
c.execute('''CREATE TABLE IF NOT EXISTS Students
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              sname TEXT NOT NULL,
              email TEXT NOT NULL UNIQUE,
              roll INTEGER NOT NULL,
              branch TEXT NOT NULL,
              batch INTEGER NOT NULL,
              password TEXT NOT NULL UNIQUE,
              joined DATE)''')


c.execute('''CREATE TABLE IF NOT EXISTS Books
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              ISBN INTEGER UNIQUE NOT NULL,
              title TEXT NOT NULL,
              author TEXT NOT NULL,
              available INTEGER)''')

c.execute('''CREATE TABLE IF NOT EXISTS Issued
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              ISBN INTEGER NOT NULL,
              book_title TEXT NOT NULL,
              author TEXT NOT NULL,
              student_email TEXT NOT NULL,
              student_roll INTEGER NOT NULL,
              issue_date DATE NOT NULL,
              FOREIGN KEY(book_title) REFERENCES Books(title),
              FOREIGN KEY(student_email) REFERENCES Students(email))''')



c.execute('''CREATE TABLE IF NOT EXISTS Returned (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_isbn INTEGER,
            book_title TEXT,
            student_roll INTEGER NOT NULL,
            student_email TEXT,
            return_date DATE,
            FOREIGN KEY (book_isbn) REFERENCES Books (isbn))''')


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect('library.db')
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()


popular_df = pickle.load(open('popularity1.pkl', 'rb'))
pt = pickle.load(open('pt.pkl', 'rb'))
books = pickle.load(open('books.pkl', 'rb'))
similarity_scores = pickle.load(open('similarity_scores.pkl', 'rb'))

@app.route('/register_student', methods=['GET', 'POST'])
def register_student():
    db = get_db()
    if request.method == 'POST':
        sname = request.form['sname']
        email = request.form['email']
        roll = request.form['roll']
        branch = request.form['branch']
        batch = request.form['batch']
        password = request.form['password']
        error = None
        if error is None:
            try:
                db.execute('INSERT INTO Students (sname, email, roll, branch, batch, password, joined) VALUES (?, ?, ?, ?, ?, ?, ?)', (sname, email, roll, branch, batch, generate_password_hash(password), datetime.date.today()))
                db.commit()
                with open('students.csv', mode='a', newline='') as file:
                    writer = csv.writer(file)
                    if file.tell() == 0:  # if the file is empty, write the table header
                        writer.writerow(['sname', 'email', 'roll', 'branch', 'batch', 'password', 'Joined'])
                    writer.writerow([sname, email, roll, branch, batch, password, datetime.date.today()])
                return redirect(url_for("login"))
            except db.IntegrityError:
                error = f"User {sname} is already registerd."
        flash(error)
    return render_template('register_student.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    db = get_db()
    if request.method == 'POST':
        rollno = request.form['roll']
        password = request.form['password']
        error = None
        student = db.execute(
            'SELECT * FROM Students WHERE roll = ?', (rollno,)
        ).fetchone()
        if student is None:
            error = 'Invalid roll number'
        elif not check_password_hash(student['password'], password):
            error = 'Invalid password'

        if error is None:
            session.clear()
            session['student_id'] = student['id']
            return redirect(url_for('index'))

        flash(error)

    return render_template('login.html')

@app.route('/register_book', methods=['GET', 'POST'])
def register_book():
    db = get_db()
    if request.method == 'POST':
        ISBN = request.form['ISBN']
        title = request.form['title']
        author = request.form['author']
        available = request.form['available']
        db.execute('INSERT INTO Books (ISBN, title, author, available) VALUES (?, ?, ?, ?)', (ISBN, title, author, available))
        db.commit()
        with open('books.csv', mode='a', newline='') as file:
            writer = csv.writer(file)
            if file.tell() == 0:  # if the file is empty, write the table header
                writer.writerow(['ISBN','title', 'author', 'available'])
            writer.writerow([ISBN, title, author, available])
        return 'Book registered successfully!'
    return render_template('register_book.html')

@app.route('/issue_book', methods=['GET', 'POST'])
def issue_book():
    db = get_db()
    if request.method == 'POST':
        ISBN = request.form['ISBN']
        student_email = request.form['student_email']
        book_title = request.form['book_title']
        author = request.form['author']
        student_roll = request.form['student_roll']
        error = None

        # Check if the student has already issued the book
        if db.execute(
                'SELECT id FROM Issued WHERE student_email = ? AND ISBN = ?', (student_email, ISBN)
        ).fetchone() is not None:
            error = f'Book {book_title} has already been issued to {student_email}'

        # Check if the book is available
        book = db.execute(
            'SELECT * FROM Books WHERE ISBN = ?', (ISBN,)
        ).fetchone()
        if book is None:
            error = f'Book with ISBN {ISBN} does not exist'
        elif book['available'] == 0:
            error = f'Book {book_title} is not available at the moment'

        if error is None:
            # Update the Issued table
            db.execute(
                'INSERT INTO Issued (ISBN, book_title, author, student_email, student_roll, issue_date) VALUES (?, ?, ?, ?, ?, ?)',
                (ISBN, book_title, author, student_email, student_roll, datetime.date.today())
            )
            # Update the Books table
            db.execute(
                'UPDATE Books SET available = ? WHERE ISBN = ?', (book['available']-1, ISBN)
            )
            try:
                db.commit()
                with open('issue_book.csv', mode='a', newline='') as file:
                    writer = csv.writer(file)
                    if file.tell() == 0:  # if the file is empty, write the table header
                        writer.writerow(['ISBN', 'book_title', 'author', 'student_email', 'student_roll', 'issue_date'])
                    writer.writerow([ISBN, book_title, author, student_email, student_roll, datetime.date.today()])
                flash(f'Book {book_title} has been issued to {student_email} successfully!')
                return redirect(url_for('index'))
            except:
                db.rollback()
                error = 'There was an error while issuing the book'

        flash(error)

    return render_template('issue_book.html')


@app.route('/return_book', methods=['GET', 'POST'])
def return_book():
    db = get_db()
    if request.method == 'POST':
        book_isbn = request.form['book_isbn']
        book_title = request.form['book_title']
        student_roll = request.form['student_roll']
        student_email = request.form['student_email']
        # Check if the student and book exist and the book is borrowed by the student
        book = db.execute('SELECT * FROM Books WHERE title = ? AND ISBN = ?', (book_title,)).fetchone()
        issued = db.execute('SELECT * FROM Issued WHERE ISBN = ? AND book_title = ? AND student_email = ? AND student_roll = ?', (book_isbn, book_title, student_email, student_roll)).fetchone()
        if issued is None:
            return 'Book not issued to this student!'
        else:
            # Update the book availability and create a new record in the Returned table
            db.execute('UPDATE Books SET available = ? WHERE ISBN = ?', (book['available']+1, book['ISBN']))
            db.execute('DELETE FROM Issued WHERE student_email = ?', (issued['student_email'],))
            db.execute('INSERT INTO Returned (book_isbn, book_id, student_roll, student_email, return_date) VALUES (?, ?, ?, ?, ?)',
                       (book['ISBN'], book['id'], issued['student_roll'], issued['student_email'], datetime.date.today()))
            db.commit()
            with open('return_book.csv', mode='a', newline='') as file:
                writer = csv.writer(file)
                if file.tell() == 0:  # if the file is empty, write the table header
                    writer.writerow(['ISBN', 'book_title', 'student_roll', 'student_email', 'return_date'])
                writer.writerow([book_isbn, book_title, student_roll, student_email, datetime.date.today()])
            
            with open('issue_book.csv', mode='r') as file:
                reader = csv.reader(file)
                issued_list = list(reader)
            with open('issue_book.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                for row in issued_list:
                    if row[0] != book_isbn or row[1] != book_title or row[2] != student_roll or row[3] != student_email:
                        writer.writerow(row)
            return 'Book returned successfully!'
        
    # Get all the borrowed books and student emails to display in the form
    issued_books = db.execute('SELECT title FROM Books WHERE available = 0').fetchall()
    emails = db.execute('SELECT email FROM Students').fetchall()
    return render_template('return_book.html', issued_books=issued_books, emails=emails)

@app.route('/')
def index():
    return render_template("index.html",
                        book_name = list(popular_df['Book-Title'].values),
                        author = list(popular_df['Book-Author'].values),
                        image = list(popular_df['Image-URL-M'].values),
                        votes = list(popular_df['num_ratings'].values),
                        rating = list(popular_df['avg_ratings'].values)
                        )

@app.route('/recommend')
def recommend_ui():
    return render_template('recommend.html')

@app.route('/recommend_books', methods=['POST'])
def recommend():
    user_input = request.form.get('user_input')
    index = np.where(pt.index == user_input)[0][0]
    similar_items = sorted(list(enumerate(similarity_scores[index])), key=lambda x:x[1], reverse=True)[1:11]
    data = []
    for i in similar_items:
        item = []
        temp_df = books[books['Book-Title'] == pt.index[i[0]]]
        item.extend(temp_df.drop_duplicates('Book-Title')['Book-Title'].values)
        item.extend(temp_df.drop_duplicates('Book-Title')['Book-Author'].values)
        item.extend(temp_df.drop_duplicates('Book-Title')['Publisher'].values)
        item.extend(temp_df.drop_duplicates('Book-Title')['Image-URL-M'].values)
        
        data.append(item)
        
    print(data)
    
    return render_template('recommend.html', data=data)

def init_db():
    db = get_db()

    with current_app.open_resource('schema.sql') as f:
        db.executescript(f.read().decode('utf8'))

@click.command('init-db')
def init_db_command():
    """Clear the existing data and create new table."""
    init_db()
    click.echo('Initialized the database.')

if __name__ == '__main__':
    app.run(debug=True)