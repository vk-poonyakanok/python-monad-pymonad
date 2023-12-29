import csv
import os
from typing import Final
from pymonad.tools import curry
from pymonad.either import Left, Right, Either

# The IO Monad class
class IO:
    def __init__(self, action):
        self.action = action
        self.result = self.execute_action()

    def execute_action(self):
        try:
            return self.action()
        except Exception as e:
            return Left(str(e))

    def bind(self, func):
        if isinstance(self.result, Either):
            if self.result.is_left():
                return self
            else:
                try:
                    return IO(lambda: func(self.result.value))
                except Exception as e:
                    return Left(str(e))
        else:
            try:
                return IO(lambda: func(self.result))
            except Exception as e:
                return Left(str(e))

    def then(self, func):
        return self.bind(func)

# Function to handle file reading, now properly wrapped in IO monad
def read_csv_file(file_path):
    def read_file():
        if not os.path.exists(file_path):
            return Left("Error: File not found")
        with open(file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            return Right([row for row in reader])
    return IO(read_file)

def remove_header(data):
    return (
        Right(data[1:]) 
        if len(data) > 1 
        else Left("Error: Unable to remove header")
    )

@curry(2)
def extract_column(column_index, data):
    return (
        Right(data).bind(lambda rows: 
        Right(list(map(lambda row: row[column_index], rows))))
    )

extract_score_column = extract_column(1)
extract_name_column = extract_column(0)

def convert_to_float(data):
    return (
        Right(list(map(float, data))) 
        if data 
        else Left("Error: Unable to convert to float")
    )


def calculate_average(column_values):
    return  (
        Right(sum(column_values) / len(column_values)) 
        if column_values 
        else Left("Error: Division by zero")
    )

# Data pipeline using the Either monad and custom sequencing operator
csv_file_path = 'example.csv'

# data =  read_csv_file(csv_file_path)

names = (
    read_csv_file(csv_file_path)
    .then (remove_header)
    .then (extract_name_column)
)

result = (
    read_csv_file(csv_file_path)
    .then (remove_header)
    .then (extract_score_column)
    .then (convert_to_float)
    .then (calculate_average) 
)

# Check if the results are Right and proceed accordingly
if names.result.is_right() and result.result.is_right():
    names = names.result.value
    average_score = result.result.value
    print(f"An average score of {', '.join(names[:-1])} and {names[-1]} is {average_score}")
else:
    print("Error processing data")