import sys
from distribution_persistence import *
from datetime import datetime as dt

DT_FORMAT_STR = '%Y-%m-%d'

def process_order_receive(p_name, p_amount, p_date):
    #Get supplier
    supplier = dist_repo.suppliers.find_by_name(p_name)

    #Get logistic
    logistic = dist_repo.logistics.find(supplier.logistic)

    #Update count received
    logistic.count_received += p_amount
    dist_repo.logistics.update(logistic)
    
    #Getting new vaccine ID
    new_vaccine_id = dist_repo.vaccines.get_maxid() + 1

    #Create new entry
    vaccine = Vaccine(new_vaccine_id, p_date, supplier.id, p_amount)
    dist_repo.vaccines.insert(vaccine)

def process_order_send(p_location, p_amount):
    #Get clinic from location and update demand
    clinic = dist_repo.clinics.find_by_location(p_location)
    logistic = dist_repo.logistics.find(clinic.logistic)

    #Get generator to iterate over vaccines by date
    vaccine_gen = dist_repo.vaccines.get_all_by_date_generator()

    #Keeping track of amount left
    amount_left = p_amount

    while(amount_left > 0):
        #Getting next vaccine. If no more left we're done.
        try:
            vaccine = next(vaccine_gen)
        except StopIteration:
            break

        #Taking maximum amount from vaccine
        amount_from_vaccine = min(vaccine.quantity, amount_left)
        amount_left -= amount_from_vaccine
        vaccine.quantity -= amount_from_vaccine

        #Checking if vaccine now empty. If so, remove.
        if(vaccine.quantity == 0):
            dist_repo.vaccines.del_by_id(vaccine.id)
        else:
            dist_repo.vaccines.update(vaccine)

    #Checking how many are left. If enough vaccines, should be full amount.
    amount_processed = p_amount - amount_left
    
    #Update clinic
    clinic.demand -= amount_processed
    dist_repo.clinics.update(clinic)

    #Update logistic
    logistic.count_sent += amount_processed
    dist_repo.logistics.update(logistic)

def get_values_for_summary():
    total_inventory = dist_repo.vaccines.get_total_quantity()
    total_demand = dist_repo.clinics.get_total_demand()
    total_sent, total_received  = dist_repo.logistics.get_total_sent_received()

    return (total_inventory, total_demand, total_received, total_sent)

def load_config_file_to_db(p_config_fname):
    with open(p_config_fname, 'r') as rfile:
        #Creating generator for split lines
        split_liter = iter(l.rstrip().split(',') for l in rfile)

        nvaccines, nsuppliers, nclinics, nlogistics = map(int, next(split_liter))

        #Load vaccines into database
        for _ in range(nvaccines):
            sid, sdate, ssupplier, squantity = next(split_liter)
            dist_repo.vaccines.insert(
                Vaccine(
                    int(sid),
                    dt.strptime(sdate, DT_FORMAT_STR),
                    int(ssupplier),
                    int(squantity)
                ))

        #Load suppliers into database
        for _ in range(nsuppliers):
            sid, sname, slogistic = next(split_liter)
            dist_repo.suppliers.insert(
                Supplier(
                    int(sid),
                    sname,
                    int(slogistic)
                ))

        #Load clinics into database
        for _ in range(nclinics):
            sid, slocation, sdemand, slogistic = next(split_liter)
            dist_repo.clinics.insert(
                Clinic(
                    int(sid),
                    slocation,
                    int(sdemand),
                    int(slogistic)
                ))

        #Load logistics into database
        for _ in range(nlogistics):
            sid, sname, scount_sent, scount_received = next(split_liter)
            dist_repo.logistics.insert(
                Logistic(
                    int(sid),
                    sname,
                    int(scount_sent),
                    int(scount_received)
                ))

def execute_orders_with_summary(p_order_fname, p_summary_fname):
    with open(p_order_fname, 'r') as rfile:
        split_liter = iter(l.rstrip().split(',') for l in rfile)

        with open(p_summary_fname, 'w') as ofile:
            for splitl in split_liter:
                #Check which type of order
                if(len(splitl) == 2 or len(splitl) == 3):
                    #Send order
                    if len(splitl) == 2:
                        process_order_send(splitl[0], int(splitl[1]))
                    #Receive order
                    else:
                        process_order_receive(splitl[0],
                                        int(splitl[1]),
                                        dt.strptime(splitl[2], DT_FORMAT_STR))

                    #Write to summary
                    summary = get_values_for_summary()
                    ofile.write(f'{summary[0]},{summary[1]},{summary[2]},{summary[3]}\n')

def main(args):
    dist_repo.create_tables()
    load_config_file_to_db(args[0])
    summary_tuples = execute_orders_with_summary(args[1], args[2])

if __name__ == '__main__':
    main(sys.argv[1:])
