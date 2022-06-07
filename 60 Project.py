import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

SOURCE = 'data2.csv'
DATA_INITIALIZATION = pd.read_csv(SOURCE)
DATA_INITIALIZATION['ship_num'] = DATA_INITIALIZATION['ship_num'].apply(str)
DATASET = {}


def alerting(hostile_ship_name,condition1 = None,condition2 = None,condition3 = None):
    hostile_ship = condition1 and condition2 and condition3
    if hostile_ship:
        ship_alert = input(f"Critical Alert!,{hostile_ship_name} are considered hostile, Continue? [Y/N] \n")
        if ship_alert.lower() == "y":
            pass
        elif ship_alert.lower() == "n":
            print("Canceling....")
            exit()
    else:
        pass

def ship_search(source,arg1, arg2, arg3, arg4=None, arg5=None, arg6=None):
    if arg1 == 1:
        return source[arg2].where(source[arg3] == arg4).dropna()
    elif arg1 == 2:
        return source[arg2].where(
            (source[arg3] == arg4) & (source[arg5] == arg6)).dropna()
    elif arg1 == 3:
        return source.where(source[arg2] == arg3).dropna()
    elif arg1 == 4:
        return source.where(
            (source[arg2] == arg3) & (source[arg4] == arg5)).dropna()


def num_of_search_parameters(data_source,arg1):
    try:
        if arg1 == 1:
            search_field = input("Select field to search by: \n")
            search_value = input("Enter value to search: \n")
            search_result = ship_search(data_source,3, search_field, search_value)
        elif arg1 == 2:
            search_field1 = input("First field to search by: \n")
            search_value1 = input("First value to search by: \n")
            search_field2 = input("Second field to search: \n")
            search_value2 = input("Second value to search: \n")
            search_result = ship_search(data_source,4, search_field1, search_value1, search_field2, search_value2)
        if "Empty DataFrame" in search_result.to_string():
            raise IndexError
        else:
            print(f"Corresponding lines: \n {search_result}")
    except IndexError:
        print("Ship not found")


def merge_with_dataset(file_type, file_path):
    try:
        if file_type == "1":
            merge_data = pd.read_csv(file_path)
        elif file_type == "2":
            merge_data = pd.read_excel(file_path)
        ship_lst = []
        alert_search = ship_search(merge_data,2,'ship_name','flag','iran')
        for row in alert_search:
          ship_lst.append(row)
        result = pd.concat([DATA_INITIALIZATION, merge_data], join="outer")
        result.to_csv(SOURCE, index=False)
        print("Merged successfully!")
    except Exception:
        print("Incorrect path")

def apply_changes_to_source(new_data,data_source):
    dataset_append = DATA_INITIALIZATION.append(new_data, ignore_index=True)
    dataset_append.to_csv(data_source, index=False)



def ship_monitor():
    action = input("Please enter choice:\n"
                         "1.Register ship entry\n"
                         "2.Register ship leaving\n"
                         "3.Search for a ship by one parameter\n"
                         "4.Search for a ship by multiple parameters\n"
                         "5.Merge\n"
                         "6.Clear file\n"
                         "9.Exit\n")
    if action == "1":
        ship_num = input("Please insert ship number: \n")
        ship_name = input("Please insert ship name: \n")
        ship_flag = input("Please insert ship flag: \n")
        ship_date = input("Please insert date: \n")
        ship_cargo = input("Please insert ship cargo: \n")
        alerting(ship_name,len(ship_name) > 6,ship_name.lower()[-1] == "x",ship_flag.lower() == "iran")
        DATASET.update(
            {
                'ship_num': ship_num,
                'ship_name': ship_name,
                'flag': ship_flag,
                'activity': "enter",
                'date': ship_date,
                'cargo': ship_cargo,
            })
        apply_changes_to_source(DATASET,SOURCE)
        print(f"{ship_name} successfully added!")
    elif action == "2":
        try:
            search = ship_search(DATA_INITIALIZATION,1, ['ship_num', 'ship_name'], 'activity', 'enter')
            search.set_index(['ship_num'], inplace=True)
            final = search.to_string()
            if "Empty DataFrame" in final:
                print("No ships entered!")
            else:
                ship_choice = input("Which ship will be leaving?\nEnter ship number: \n")
                list_of_entry = ship_search(DATA_INITIALIZATION,1, ['activity'], 'ship_num', ship_choice)
                result_to_string = list_of_entry.iloc[-1].to_string()
                if "leave" in result_to_string:
                    print("Ship need to enter first!")
                else:
                    ship_leave_date = input("When will the ship be leaving?")
                    ship_entry_date = ship_search(DATA_INITIALIZATION,2, 'date', 'ship_num', ship_choice, 'activity', 'enter')
                    ship_leave_cargo = input("Ship cargo?: \n")
                    ship_entry_cargo = ship_search(DATA_INITIALIZATION,2, 'cargo', 'ship_num', ship_choice, 'activity', 'enter')
                    ship_details = ship_search(DATA_INITIALIZATION,3, 'ship_num', ship_choice)
                    data_replace = ship_details.iloc[-1].replace(
                        ["enter", ship_entry_cargo.iloc[-1], ship_entry_date.iloc[-1]],
                        ["leave", ship_leave_cargo, ship_leave_date])
                    apply_changes_to_source(data_replace,SOURCE)
                    print(f"{ship_choice} left the port")
        except IndexError:
            print("Ship not found")
    elif action == "3":
        num_of_search_parameters(DATA_INITIALIZATION,1)
    elif action == "4":
        num_of_search_parameters(DATA_INITIALIZATION,2)
    elif action == "5":
        file_type = input("Choose option: \n"
                          "1.cvs\n"
                          "2.Excel\n")
        file_path = input("Insert file path:\n")
        merge_with_dataset(file_type, file_path)
    elif action == "6":
        alert = input("Doing this will clear all rows!\n"
                      "are you sure? [Y/N]\n")
        if alert == "Y" or alert == "y":
            clear_dataset = {
                'ship_num': [],
                'ship_name': [],
                'flag': [],
                'date': [],
                'activity': [],
                'cargo': []
            }
            apply_clear = pd.DataFrame(clear_dataset)
            apply_clear.to_csv(SOURCE, index=False)
            print("File cleared!")
            exit()
        elif alert == "N" or alert == "n":
            print("Canceled...")


ship_monitor()
