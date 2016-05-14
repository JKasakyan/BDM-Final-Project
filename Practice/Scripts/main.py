# hdfs:///gws/classes/bdma/ccny/groups/3/NYPD_Motor_Vehicle_Collisions.csv
# hdfs:///gws/classes/bdma/ccny/groups/3/311_Service_Requests_from_2010_to_Present.csv

import sys

def main(sc, accident_csv, three_one_one_csv):
    from Police_Reports import get_rdd as get_accident_rdd
    from Three_One_One_Reports import get_rdd as get_three_one_one_rdd

    accident_rdd      = get_accident_rdd(sc, accident_csv)
    three_one_one_rdd = get_three_one_one_rdd(sc, three_one_one_csv)
    joined_rdd        = accident_rdd.join(three_one_one_rdd)

    def mapper(tuples):
        for t in tuples:
            zip_code = t[0]
            acc_t = t[1][0]
            three_t = t[1][1]
            tuple_type = type(())
            if type(three_t) != tuple_type  or type(acc_t) != tuple_type :
                continue
            # accident dataset
            t_accidents = acc_t[0]
            t_vehicles = acc_t[1]
            t_per_i =  acc_t[2]
            t_per_k =  acc_t[3]
            t_ped_i = acc_t[4]
            t_ped_k =  acc_t[5]
            t_cyc_i = acc_t[6]
            t_cyc_k = acc_t[7]
            t_mot_i = acc_t[8]
            t_mot_k = acc_t[9]
            fac1 = acc_t[10]
            fac2 = acc_t[11]
            fac3 = acc_t[12]
            fac4 = acc_t[13]
            fac5 = acc_t[14]
            veh1 = acc_t[15]
            veh2 = acc_t[16]
            veh3 = acc_t[17]
            veh4 = acc_t[18]
            veh5 = acc_t[19]
            # 311 dataset
            t_complaints = three_t[0]
            comp1 = three_t[1]
            comp2 = three_t[2]
            comp3 = three_t[3]
            desc1 = three_t[4]
            desc2 = three_t[5]
            desc3 = three_t[6]
            desc4 = three_t[7]
            desc5 = three_t[8]

            yield (zip_code, t_accidents, t_vehicles, t_complaints,
                   t_per_i, t_ped_i, t_cyc_i, t_mot_i,
                   t_per_k, t_ped_k, t_cyc_k, t_mot_k,
                   fac1, fac2, fac3, fac4, fac5,
                   veh1, veh2, veh3, veh4, veh5,
                   comp1, comp2, comp3,
                   desc1, desc2, desc3, desc4, desc5)

    def toCSVLine(data):
        return ','.join(str(d) for d in data)

    joined_rdd.mapPartitions(mapper).map(toCSVLine).coalesce(1).saveAsTextFile("./Results/final_results")

if __name__ == "__main__":
        arguments = sys.argv
        accident_csv = arguments[1]
        three_one_one_csv = arguments[2]
        main(sc, accident_csv, three_one_one_csv)
