import sys
from distribution_persistence import *
from datetime import datetime as dt

DT_FORMAT_STR = '%Y-%m-%d'

def main(args):
    with open(args[0], 'r') as rfile:
        liter = iter(rfile.readlines())

    nvaccines, nsuppliers, nclinics, nlogistics = map(int, next(liter).split(','))

    for i in range(nvaccines):
        sid, sdate, ssupplier, squantity = next(liter).split(',')
        dist_repo.vaccines.insert(
            Vaccine(
                int(sid),
                dt.strptime(sdate, DT_FORMAT_STR),
                int(ssupplier),
                int(squantity)
            ))

    for i in range(nsuppliers):
        sid, sname, slogistic = next(liter).split(',')
        dist_repo.suppliers.insert(
            Supplier(
                int(sid),
                sname,
                int(slogistic)
            ))

    for i in range(nclinics):
        sid, slocation, sdemand, slogistic = next(liter).split(',')
        dist_repo.clinics.insert(
            Clinic(
                int(sid),
                slocation,
                int(sdemand),
                int(slogistic)
            ))
    
    for i in range(nlogistics):
        sid, sname, scount_sent, scount_received = next(liter).split(',')
        dist_repo.logistics.insert(
            Logistic(
                int(sid),
                sname,
                int(scount_sent),
                int(scount_received)
            ))
if __name__ == '__main__':
    main(sys.argv[1:])
