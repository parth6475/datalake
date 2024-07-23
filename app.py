from flask import Flask, render_template, request, redirect, session
from pymongo import MongoClient
from load_data_into_mongodb import insert_data
from load_into_mysql import insert_data_mysql
from read_data_from_mongodb import find_userdata
import mysql.connector
import boto3

# S3 Bucket connection
S3_BUCKET = 'secrettest6969'
S3_KEY = 'AKIASVTIBUZLS3QJRPDV'
S3_SECRET = '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'


# MySql connection
cnx = mysql.connector.connect(user='root', password='0101',host='127.0.0.1',database='datalake')
cursor = cnx.cursor()


# MongoDB connection
# client=MongoClient("mongodb://localhost:27017/") 
# db = client['datalake']
# collection = db['user1']


app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

@app.route("/")
def hello():
	return render_template('login.html')

@app.route("/home_page")
def home_page():
	return render_template('index.html')


# ******* this is for home page button ******* 
@app.route("/home", methods=['POST'])
def home():
    username = request.form['username']
    password = request.form['password']

    cursor.execute("SELECT * FROM id_pass WHERE username=%s AND password=%s", (username, password))
    user = cursor.fetchone()

    if user:
        session['username'] = username
        return render_template('index.html')
    else:
        return redirect('/?error=Invalid_username_or_password')


# ******* this is after user login index page button ******* 
@app.route("/choose_dataform", methods=['GET', 'POST'])
def choose_dataform():
    return render_template('choose_dataform.html')

@app.route("/choose", methods=['GET', 'POST'])
def choose():
    return render_template('choose.html')

@app.route("/manage_user_permission", methods=['GET', 'POST'])
def manage_permission():
    return render_template('manage_permission.html')



@app.route("/personal_info", methods=['GET', 'POST'])
def personal_info():
    return render_template('personal_info.html')

@app.route("/personal_files", methods=['GET', 'POST'])
def personal_files():
    return render_template('upload_files.html')








# ******* this is for display Data *******
@app.route("/display_information", methods=['GET', 'POST'])
def my_info():
    username = session.get('username')
    session['option'] = request.form.get('option')
    if request.form.get('user')=='my':
        # ******* this is for display information from MongoDB *******    
        if request.form.get('option')=='information':
            data=find_userdata(username)
            return render_template('display_info.html',user=username,results=data)
        # ******* this is for display files from S3 *******
        else:
            s3 = boto3.client('s3', aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET)
            folder_name = username
            print(folder_name)
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f'{folder_name}/')
            images = []
            for obj in response.get('Contents', []):
                image_url = f'https://{S3_BUCKET}.s3.amazonaws.com/{obj.get("Key")}'
                images.append(image_url)
            return render_template('display_files.html', images=images)
    else:
        if request.form.get('option')=='information':
            cursor.execute("SELECT access_name FROM permission WHERE u_name=%s", (username,))
        else:
            cursor.execute("SELECT access_name FROM permission_files WHERE u_name=%s", (username,))
        other_user = cursor.fetchall()
        return render_template('choose_user.html',user_options=other_user)


@app.route("/display_information_of_other", methods=['POST'])
def other_user_info():
    selected_user = request.form['user']
    if session.get('option')=='information':
        data=find_userdata(selected_user)
        return render_template('display_info.html',user=selected_user,results=data)
    else:
        s3 = boto3.client('s3', aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET)
        folder_name = selected_user
        print(folder_name)
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f'{folder_name}/')
        images = []
        for obj in response.get('Contents', []):
            image_url = f'https://{S3_BUCKET}.s3.amazonaws.com/{obj.get("Key")}'
            images.append(image_url)
        return render_template('display_files.html', images=images)








# ******* this is for data store in mongo *******
@app.route("/DataStore", methods=['POST'])
def storedataIntoMongo():    
    username = session.get('username')
    # name = request.form['name']
    email = request.form['email']
    age = request.form['age']
    hobby = request.form['hobby']
    insert_data(username, email, age, hobby)
    # insert_data_mysql(username)
    return render_template('index.html')

# ******* this is for files store in s3 *******
@app.route('/upload_file', methods=['POST'])
def upload_file_to_s3():
    folder_name=session.get('username')
    file = request.files['file']
    s3 = boto3.client('s3', aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET)
    s3.put_object(Bucket=S3_BUCKET, Key=f'{folder_name}/{file.filename}')
    s3.upload_fileobj(file, S3_BUCKET, f'{folder_name}/{file.filename}')
    return render_template('index.html')






# ******* this is for manage permission of every user *******
@app.route("/operation", methods=['POST'])
def select_operation():
    username = session.get('username')
    operation = request.form['operation']
    database = request.form['database']
    session['database'] = database
    cursor.execute("SELECT u_name FROM user WHERE u_name!=%s", (username,))
    all_user = cursor.fetchall()
    remaining_uesr=[]
    for result in all_user:
        remaining_uesr.append(result[0])
    if operation=='add':
        if database=='mongodb':
            cursor.execute("SELECT u_name FROM permission WHERE access_name=%s", (username,))
            access_user = cursor.fetchall()
            duplicate_user=[]
            for result in access_user:
                duplicate_user.append(result[0])
            for s in duplicate_user:
                if s in remaining_uesr:
                    remaining_uesr.remove(s)
        else:
            cursor.execute("SELECT u_name FROM permission_files WHERE access_name=%s", (username,))
            access_user = cursor.fetchall()
            duplicate_user=[]
            for result in access_user:
                duplicate_user.append(result[0])
            for s in duplicate_user:
                if s in remaining_uesr:
                    remaining_uesr.remove(s)
        return render_template('add_user_permission.html',users=remaining_uesr)
    else:
        if database=='mongodb':
            cursor.execute("SELECT u_name FROM permission WHERE access_name=%s", (username,))
        else:
            cursor.execute("SELECT u_name FROM permission_files WHERE access_name=%s", (username,))
        access_user = cursor.fetchall()
        return render_template('remove_user_permission.html',users=access_user)


@app.route("/add_user_permission", methods=['POST'])
def add_user_permission():
    username = session.get('username')
    add_users = request.form.getlist('selected_users')
    # print(add_users)
    database = session.get('database')
    if database=='mongodb':
        for user in add_users:
            # Insert the user into the MySQL database
            cursor.execute("INSERT INTO permission (u_name, access_name) VALUES (%s, %s)", (user,username))
            cnx.commit()
    else:
        for user in add_users:
            # Insert the user into the MySQL database
            cursor.execute("INSERT INTO permission_files (u_name, access_name) VALUES (%s, %s)", (user,username))
            cnx.commit()
    return render_template('index.html')


@app.route("/remove_user_permission", methods=['POST'])
def remove_user_permission():
    username = session.get('username')
    remove_users = request.form.getlist('selected_users')
    database = session.get('database')
    if database=='mongodb':
        for user in remove_users:
            cursor.execute("DELETE FROM permission WHERE u_name=%s AND access_name=%s", (user,username))
            cnx.commit()
    else:
        for user in remove_users:
            cursor.execute("DELETE FROM permission_files WHERE u_name=%s AND access_name=%s", (user,username))
            cnx.commit()
    return render_template('index.html')



if __name__ == '__main__':
	app.run(debug = True)