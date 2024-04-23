from flask import Flask, render_template, request, redirect, url_for
from logic.db_logic import connect_to_database, create_cursor, upload_files, create_tables, standard_tables_display, standard_tables_display, dynamic_tables_display, generate_charts
import csv
import plotly.express as px


app = Flask(__name__)

# Establish database connection
db = connect_to_database()

# Create tables in the database
create_tables(db)

# Create a cursor object
# cursor = db.cursor()


@app.route('/')
def main_page():
    return render_template('main_page.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        return render_template('login_details.html', username=username, email=email, password=password)
    return render_template('login.html')


@app.route('/upload', methods=['GET', 'POST'])
def upload_page():
    if request.method == 'POST':
        file1 = request.files.get('file1')
        file2 = request.files.get('file2')
        file3 = request.files.get('file3')

        # Check if all three files are present
        if not all([file1, file2, file3]):
            return redirect(url_for('upload_page'))

        # Process the files
        files = [file1, file2, file3]
        success_message = upload_files(files, db)
        return redirect(url_for('upload_page'))
    return render_template('upload.html')


#q3
@app.route('/standard_display')
def standard_display():
    headers, results = standard_tables_display(db)
    return render_template('standard_display.html', results=results,headers=headers)

#q4
@app.route('/query', methods=['GET', 'POST'])
def query_page():
    if request.method == 'POST':
        hshd_num = request.form.get('hshd_num')

        # Query the database based on the input hshd_num
        headers, results = dynamic_tables_display(db, hshd_num)
        return render_template('query_page.html', results=results, headers=headers)

    return render_template('query_page.html', results=None, headers=None)

# from logic.db_logic import fig_household_income_html, fig_children_spend_html,fig_household_size_spend_html,fig_marital_status_spend_html,fig_age_range_spend_html 
# Define routes
@app.route('/dashboard')
def dashboard():
    # fig_household_income, fig_children_spend, fig_household_size_spend, fig_marital_status_spend, fig_age_range_spend = generate_charts(db)
    # return render_template('dashboard.html', fig_household_income=fig_household_income, fig_children_spend=fig_children_spend,
    #                        fig_household_size_spend=fig_household_size_spend, fig_marital_status_spend=fig_marital_status_spend,
    #                        fig_age_range_spend=fig_age_range_spend)

    fig_household_income_html, fig_children_spend_html, fig_household_size_spend_html, fig_marital_status_spend_html, fig_age_range_spend_html = generate_charts(db)
    return render_template('dashboard.html', fig_household_income=fig_household_income_html,
                           fig_children_spend=fig_children_spend_html,
                           fig_household_size_spend=fig_household_size_spend_html,
                           fig_marital_status_spend=fig_marital_status_spend_html,
                           fig_age_range_spend=fig_age_range_spend_html)


if __name__ == '__main__':
    app.run(debug=True)
