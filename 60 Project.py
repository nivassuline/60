import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

source = 'data1.csv'
DATA_INITIALIZATION = pd.read_csv(source)
DATASET = {}




def ship_search(arg1,arg2,arg3,arg4 = None,arg5 = None,arg6 = None):
    if arg1 == 1:
        return DATA_INITIALIZATION[arg2].where(DATA_INITIALIZATION[arg3] == arg4).dropna()
    elif arg1 == 2:
        return DATA_INITIALIZATION[arg2].where((DATA_INITIALIZATION[arg3] == arg4) & (DATA_INITIALIZATION[arg5] == arg6)).dropna()
    elif arg1 == 3:
        return DATA_INITIALIZATION.where(DATA_INITIALIZATION[arg2] == arg3).dropna()
    elif arg1 == 4:
        return DATA_INITIALIZATION.where((DATA_INITIALIZATION[arg2] == arg3) & (DATA_INITIALIZATION[arg4] == arg5)).dropna()


def ship_search_choice(arg1):
    if arg1 == 1:
        search_field = input("Select field to search by: \n")
        search_value = input("Enter value to search: \n")
        DATA_INITIALIZATION['ship_num'] = DATA_INITIALIZATION['ship_num'].apply(str)
        search_result = ship_search(3, search_field, search_value)
        if "Empty DataFrame" in search_result.to_string():
            raise Exception
        else:
            print(f"Corresponding lines: \n {search_result}")
    elif arg1 == 2:
        search_field1 = input("First field to search by: \n")
        search_value1 = input("First value to search by: \n")
        search_field2 = input("Second field to search: \n")
        search_value2 = input("Second value to search: \n")
        DATA_INITIALIZATION['ship_num'] = DATA_INITIALIZATION['ship_num'].apply(str)
        search_result = ship_search(4, search_field1, search_value1, search_field2, search_value2)
        if "Empty DataFrame" in search_result.to_string():
            raise Exception
        else:
            print(f"Corresponding lines: \n {search_result}")


def ship_monitor():
    first_choice = input("Please enter choice:\n"
                         "1.Register ship entry\n"
                         "2.Register ship leaving\n"
                         "3.Search for a ship by one parameter\n"
                         "4.Search for a ship by multiple parameters\n"
                         "5.Merge\n"
                         "6.Clear file\n"
                         "9.Exit\n")
    if first_choice == "1":
        ship_num = input("Please insert ship number: \n")
        ship_name = input("Please insert ship name: \n")
        ship_flag = input("Please insert ship flag: \n")
        ship_date = input("Please insert date: \n")
        ship_cargo = input("Please insert ship cargo: \n")
        if len(ship_name) > 6 and ship_name.lower()[-1] == "x" and ship_flag.lower() == "iran":
            ship_alert = input(f"Critical Alert!,{ship_name} is considered hostile, Continue? [Y/N] \n")
            if ship_alert.lower() == "y":
                pass
            elif ship_alert.lower() == "n":
                print("Canceling....")
                exit()
        DATASET.update(
            {
                'ship_num': ship_num,
                'ship_name': ship_name,
                'flag': ship_flag,
                'activity': "enter",
                'date': ship_date,
                'cargo': ship_cargo,
            })
        dataset_append = DATA_INITIALIZATION.append(DATASET, ignore_index=True)
        dataset_append.to_csv(source, index=False)
        print(f"{ship_name} successfully added!")
    elif first_choice == "2":
        try:
            DATA_INITIALIZATION['ship_num'] = DATA_INITIALIZATION['ship_num'].apply(str)
            search = ship_search(1,['ship_num','ship_name'],'activity','enter')
            search.set_index(['ship_num'], inplace=True)
            final = search.to_string()
            if "Empty DataFrame" in final:
                print("No ships entered!")
            else:
                ship_choice = input("Which ship will be leaving?\nEnter ship number: \n")
                list_of_entry = ship_search(1,['activity'],'ship_num',ship_choice)
                result_to_string = list_of_entry.iloc[-1].to_string()
                if "leave" in result_to_string:
                    print("Ship need to enter first!")
                else:
                    ship_leave_cargo = input("Ship cargo?: \n")
                    ship_enter_cargo = ship_search(2,'cargo','ship_num',ship_choice,'activity','enter')
                    ship_details = ship_search(3,'ship_num',ship_choice)
                    ship_replace = ship_details.iloc[-1].replace(["enter",ship_enter_cargo.iloc[-1]],["leave",ship_leave_cargo])
                    dataset_append = DATA_INITIALIZATION.append(ship_replace, ignore_index=True)
                    dataset_append.to_csv(source, index=False)
                    print(f"{ship_choice} left the port")
        except Exception:
             print("Ship not found")
    elif first_choice == "3":
        try:
            ship_search_choice(1)
        except Exception:
            print("Ship not found")
    elif first_choice == "4":
        try:
            ship_search_choice(2)
        except Exception:
            print("Ship not found")
    elif first_choice == "5":
        try:
            file_option = input("Choose option: \n"
                                "1.cvs\n"
                                "2.Excel\n")
            if file_option == "1":
                file_path = input("Insert file path:\n")
                merge_data = pd.read_csv(file_path)
                result = pd.concat([DATA_INITIALIZATION, merge_data], join="outer")
                result.to_csv(source, index=False)
                print("Merged successfully!")
            elif file_option == "2":
                file_path = input("Insert file path:\n")
                merge_data = pd.read_excel(file_path)
                result = pd.concat([DATA_INITIALIZATION, merge_data], join="outer")
                result.to_csv(source, index=False)
                print("Merged successfully!")
        except Exception:
            print("Incorrect path")
    elif first_choice == "6":
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
            apply_clear.to_csv(source, index=False)
            print("File cleared!")
            exit()
        elif alert == "N" or alert == "n":
            print("Canceled...")


ship_monitor()
