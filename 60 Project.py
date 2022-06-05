import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

DATASET = {}
data_source = pd.read_csv('data1.csv')


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
        dataset_append = data_source.append(DATASET, ignore_index=True)
        dataset_append.to_csv('data1.csv', index=False)
        print(f"{ship_name} successfully added!")
    elif first_choice == "2":
        try:
            data_source['ship_num'] = data_source['ship_num'].apply(str)
            search = data_source[["ship_num", "ship_name"]].where((data_source['activity'] == 'enter')).dropna()
            search.set_index(['ship_num'], inplace=True)
            final = search.to_string()
            if "Empty DataFrame" in final:
                print("No ships entered!")
            else:
                print(final)
                ship_choice = input("Which ship will be leaving?\nEnter ship number: \n")
                list_of_entry = data_source[["activity"]].where(data_source['ship_num'] == ship_choice).dropna()
                result_to_string = list_of_entry.iloc[-1].to_string()
                if "leave" in result_to_string:
                    print("Ship need to enter first!")
                else:
                    ship_details = data_source.where(data_source['ship_num'] == ship_choice).dropna()
                    ship_details.iloc[1].replace(to_replace="enter",value="leave")
                    print(ship_details.iloc[1])
                    dataset_append = data_source.append(ship_details, ignore_index=True)
                    dataset_append.to_csv('data1.csv', index=False)
                    print(f"{ship_choice} left the port")
        except Exception:
            print("Ship not found")
    elif first_choice == "3":
        try:
            search_field = input("Select field to search by: \n")
            search_value = input("Enter value to search: \n")
            data_source['ship_num'] = data_source['ship_num'].apply(str)
            search_result = data_source.where(data_source[search_field] == search_value).dropna()
            if "Empty DataFrame" in search_result.to_string():
                raise Exception
            else:
                print(f"Corresponding lines: \n {search_result}")
        except Exception:
            print("Ship not found")
    elif first_choice == "4":
        try:
            search_field1 = input("First field to search by: \n")
            search_value1 = input("First value to search by: \n")
            search_field2 = input("Second field to search: \n")
            search_value2 = input("Second value to search: \n")
            data_source['ship_num'] = data_source['ship_num'].apply(str)
            search_result = data_source.where(
                (data_source[search_field1] == search_value1) & (
                        data_source[search_field2] == search_value2)).dropna()
            if "Empty DataFrame" in search_result.to_string():
                raise Exception
            else:
                print(f"Corresponding lines: \n {search_result}")
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
                result = pd.concat([data_source, merge_data], join="outer")
                result.to_csv('data1.csv', index=False)
                print("Merged successfully!")
            elif file_option == "2":
                file_path = input("Insert file path:\n")
                merge_data = pd.read_excel(file_path)
                result = pd.concat([data_source, merge_data], join="outer")
                result.to_csv('data1.csv', index=False)
                print("Merged successfully!")
        except Exception:
            print("Incorrect path")
    elif first_choice == "6":
        alert = input("Doing this will clear all values!\n"
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
            apply_clear.to_csv('data1.csv', index=False)
            print("File cleared!")
            exit()
        elif alert == "N" or alert == "n":
            print("Canceled...")


ship_monitor()
