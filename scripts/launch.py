import colorama
from colorama import Fore, Style, Back
from query_functions import *

colorama.init(autoreset=True)

# PROCESSING TEXT
def process_text(input_text):
    qp = query_pipeline(input_text)
    if 'iris' in input_text:
        qp.set_chunksize(1000)
    else: 
        qp.set_chunksize(4000000)
    qp.getFlags()
    out_gen = qp.evaluateQuery()
    return out_gen

# WELCOMING MESSAGE
def welcome():
    print(Fore.YELLOW + Fore.RED + "Welcome to CatDB!" + Style.RESET_ALL)

# GOODBYE MESSAGE
def goodbye():
    print(Fore.YELLOW + "Goodbye!" + Style.RESET_ALL)

# MAIN FUNCTION
welcome()

while True:
    # use input() to get text input
    user_input = input(Fore.LIGHTYELLOW_EX + Fore.RED + "Enter text (or type 'exit' to end): \n" + Style.RESET_ALL)

    # if input is exit, stop the CLI
    if user_input == 'exit':
        goodbye()
        break

    # process input
    delete_everythingInFolder('../data/tmp')
    out_gen = process_text(user_input)

    print(Fore.YELLOW + "Query Result: \n" + Style.RESET_ALL)

    for i in out_gen:
        print(i)


