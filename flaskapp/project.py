from flask import Flask, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename
import shutil
import csv
import os
import pandas as pd
import numpy as np 
import matplotlib
import matplotlib.pyplot as plt



#FLASK APP

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config.update(dict(
    SECRET_KEY="powerful secretkey",
    WTF_CSRF_SECRET_KEY="a csrf secret key"))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'data.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
DATASETS_DIRECTORY = 'datasets'

#DATA MODEL

class Dataset(db.Model):

    __tablename__ = 'dataset'

    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(80), unique=True, nullable=False)
    directory = db.Column(db.String(80), unique=True, nullable=False)
    number_of_lines = db.Column(db.Integer, nullable=False)
    columns_separator = db.Column(db.String(1), nullable=False)
    number_of_columns = db.Column(db.Integer, nullable=False)
    columns_name = db.Column(db.String(1200), nullable=False)
    columns_type = db.Column(db.String(600), nullable=False)
    columns_description_filename = db.Column(db.String(80), unique=True, nullable=False)

    def __repr__(self):
        return f'<Dataset {self.filename}, ({self.number_of_lines} x {self.number_of_columns})>'

#ROUTES

@app.route('/')
def index():
    return render_template('index.html')

def get_file_info(filename, columns_separator, coltypes=None, colnames=None):

    dir_name = filename.split('.')[0]

    info = ''

    columns_filename = f'{DATASETS_DIRECTORY}/{dir_name}/{dir_name}_column_description.csv'
    if colnames:
        df = pd.read_csv(f'{DATASETS_DIRECTORY}/{dir_name}/{filename}', names=colnames, sep=columns_separator)
    else:
        df = pd.read_csv(f'{DATASETS_DIRECTORY}/{dir_name}/{filename}', sep=columns_separator)
        colnames = list(df.columns)

    print(type(coltypes))
    if coltypes !="":
        for column, types in zip(df, coltypes):
            try:
                if types == 'date':
                    df[column] = pd.to_datetime(df[column])
                elif types == 'float':
                    df[column] = float(df[column].replace('.',','))
                else:
                    df[column] = df[column].astype(types)
                    print(df[column].dtypes)
            except ValueError:
                info += f'Error when converting {column} to {types}'
                df[column] = df[column].astype(str)

    filename = dir_name + ".pkl"
    df.to_pickle(f'{DATASETS_DIRECTORY}/{dir_name}/{filename}')

    with open(columns_filename, 'w', newline='') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=' ',
                                escapechar=' ', quoting=csv.QUOTE_NONE)
        print(df)
        for column in df:
            print(column)
            if df[column].dtypes == 'object':
                datawriter.writerow([get_object(df,column)])

            elif df[column].dtypes == 'bool':
                datawriter.writerow([get_bool(df,column,dir_name)])

            elif df[column].dtype.name == 'category':
                datawriter.writerow([get_cat(df,column,dir_name)])

            elif np.issubdtype(df[column].dtype, np.number):
                datawriter.writerow([get_nums(df,column,dir_name)])

            elif df[column].dtypes == 'datetime64[ns]':
                datawriter.writerow([get_date(df,column)])

    lines = len(df)
    columns = len(df.columns)

    return info, Dataset(
        filename=filename,
        directory=os.path.join(DATASETS_DIRECTORY, dir_name),
        number_of_lines=lines,
        number_of_columns=columns,
        columns_separator=columns_separator,
        columns_name=str(colnames),
        columns_type=str(coltypes),
        columns_description_filename=columns_filename)

@app.route('/datasets/add', methods=['GET', 'POST'])
def add_dataset():
    return render_template('add_dataset.html')

@app.route('/datasets/upload_result', methods=['GET','POST'])
def upload_result():

    if request.method == 'POST':
        columns_separator = request.form.get('col_sep', ';')
        checkbox = request.form.get('checkbox')
        colnames = request.form.get('colnames')
        if colnames:
            colnames = colnames.split(';')
        coltypes = request.form.get('coltypes')
        if coltypes:
            coltypes = coltypes.split(';')

        f = request.files["yourfile"]
        filename = secure_filename(f.filename)
        file_dirname = filename.split('.')[0]
        data_dir = r'datasets'
        if file_dirname in os.listdir(data_dir):
            return render_template('upload_result.html', info=f'File {filename} already exist')
        else:
            try:
                os.mkdir(f'{data_dir}/{file_dirname}')
            except Exception as e:
                return render_template('upload_result.html',info='Cannot create folder')
            file_path_dir = f'{data_dir}/{file_dirname}/{filename}'

            f.save(file_path_dir)

            info, dataset_object = get_file_info(filename, columns_separator, coltypes, colnames)
            print(dataset_object)
            if os.path.exists(file_path_dir):
                try:
                    os.remove(file_path_dir)
                except: 
                    pass
            if db.session.query(Dataset).filter_by(filename=dataset_object.filename).count() < 1:
                db.session.add(dataset_object)
                db.session.commit()

        return render_template('upload_result.html', dataset=dataset_object, info=info)

@app.route('/datasets/details/<filename>', methods=['GET', 'POST'])
def details(filename):
    dir_name = filename.split('.')[0]
    if os.path.isfile(f'{DATASETS_DIRECTORY}/{dir_name}/{dir_name}_column_description.csv'):
        try:
            df = pd.read_csv(f'{DATASETS_DIRECTORY}/{dir_name}/{dir_name}_column_description.csv',
                names=['name','type','unique','null','min',
                        'avg','max','median','std','oldest','newest','histogram'], sep=';', header=None)
        except:
            df = pd.read_csv(f'{DATASETS_DIRECTORY}/{dir_name}/{dir_name}_column_description.csv',
                names=['name','type','unique','null','min',
                        'avg','max','median','std','oldest','newest','histogram'], sep=';', header=None, encoding='latin1')
        return render_template("details.html", tables = df)
    return render_template("details.html", info = "File not found")


@app.route('/datasets/list', methods=['GET', 'POST'])
def list_datasets():
    datasets = db.session.query(Dataset).all()
    return render_template('list_datasets.html', datasets=datasets)


@app.route('/datasets/delete/<int:dataset_id>')
def delete_user(dataset_id):
    data_dir = r'datasets'

    dataset_to_remove = db.session.query(Dataset).filter(Dataset.id == dataset_id).first()
    if dataset_to_remove is None:
        return render_template("delete.html", info=f"File with ID {dataset_id} not found.")
    else:
        filename = db.session.query(Dataset.filename).filter(Dataset.id == dataset_id).first()
        dir_name = filename[0].split('.')[0]
        if os.path.isdir(f'{data_dir}/{dir_name}'): 
            shutil.rmtree(f'{data_dir}/{dir_name}')
        db.session.delete(dataset_to_remove)
        db.session.commit()
        return render_template("delete.html", dataset=dataset_to_remove)


#VIZ AND STATS

def histogram(df,column,dir_name):
    plt.figure(figsize=(8, 8))
    fig = plt.hist(df[column], bins=50)
    plt.grid(axis='y', alpha=0.75)
    plt.xlabel('Unit', fontsize=15)
    plt.ylabel('Frequency', fontsize=15)
    plt.xlim(0, max(df[column]))
    plt.grid(axis='y', alpha=0.75)
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    plt.ylabel('Frequency', fontsize=15)
    plt.title(column.upper(), fontsize=15)
    plt.savefig(f'{DATASETS_DIRECTORY}/{dir_name}/{column}_hist.png')

def get_object(df,column):
    nulls = df[column].isnull().sum()
    types = df[column].dtypes
    uniques = df[column].nunique()
    return f'{column};{types};{uniques};{nulls};'

def get_nums(df,column,dir_name):
    desc = df[column].describe().loc[['min', 'mean', 'max', '50%', 'std']]
    types = df[column].dtypes
    histogram(df,column, dir_name)
    return f'{column};{types};{desc[0]};{desc[1]};{desc[2]};{desc[3]};{desc[4]};{DATASETS_DIRECTORY}/{dir_name}/{column}_hist.png'

def get_cat(df,column,dir_name):
    uniques = df[column].nunique()
    types = df[column].dtypes
    histogram(df,column, dir_name)
    return f'{column};{types};{uniques};{DATASETS_DIRECTORY}/{dir_name}/{column}_hist.png'

def get_bool(df,column,dir_name):
    types = df[column].dtypes
    fig = plt.hist((df[column].astype(float)), bins=30)
    plt.title(column)
    plt.xlabel("Unit")
    plt.ylabel("Frequency")
    plt.savefig(column + '_hist.png')
    return f'{column};{types};{DATASETS_DIRECTORY}/{dir_name}/{column}_hist.png'

def get_date(df,column):
    types = df[column].dtypes
    mini = df[column].min()
    maxi = df[column].max()
    return f'{column};{types};{mini};{maxi};'

#MAIN

if __name__=="__main__":
    db.create_all()
    if not os.path.exists(DATASETS_DIRECTORY):
        os.makedirs(DATASETS_DIRECTORY)
    app.run(debug=True, port=5000)