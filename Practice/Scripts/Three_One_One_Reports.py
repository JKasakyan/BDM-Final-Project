
def get_rdd(sc):
    from datetime import datetime
    from dateutil.parser import parse
    from heapq import nlargest
    rdd = sc.textFile("../Datasets/311_Service_Requests_from_2010_to_Present.csv")
    header = rdd.first()

    def select_fields(records):
        first_day_2012 = datetime(year=2012, month=1, day=1, hour=0, minute=0)
        last_day_2013 = datetime(year=2013, month=12, day=31, hour=23, minute=59)
        for record in records:
            if record == header:
                continue
            fields = record.split(',')
            complaint_type = fields[5]
            zip_code = fields[8]
            try:
                date_parsed = parse(fields[1])
            except ValueError:
                continue
            try:
                zip_code = int(fields[8])
                descriptor = fields[6]
            except ValueError:
                if zip_code == "":
                    continue
                elif "Street Sign" in complaint_type:
                    zip_code = fields[10]
                    descriptor = "{} {} {}".format(fields[6], fields[7], fields[8])
                else:
                    zip_code = fields[9]
                    descriptor = "{} {}".format(fields[6], fields[7])
            if (first_day_2012 <= date_parsed <= last_day_2013 and zip_code != "" and "Street" in complaint_type and "Noise" not in complaint_type):
                yield (str(zip_code), (str(complaint_type).upper(), str(descriptor)))

    filtered_rdd = rdd.mapPartitions(select_fields)

    def seqOp(agg_dict, record):
        complaint_type = record[0]
        descriptor = record[1]
        agg_dict["total_complaints"] = agg_dict.get("total_complaints", 0) + 1
        agg_dict[complaint_type] = agg_dict.get(complaint_type, 0) + 1
        agg_dict[descriptor] = agg_dict.get(descriptor, 0) + 1
        return agg_dict

    def combOp (dict1, dict2):
        for key, value in dict1.items():
            dict2[key] = dict2.get(key, 0) + value
        return dict2

    zip_complaints_rdd = filtered_rdd.aggregateByKey({}, seqOp, combOp)

    def topn(zip_tuples):
        for zip_tuple in zip_tuples:
            zip_code = zip_tuple[0]
            zip_dict = zip_tuple[1]
            total_complaints = float(zip_dict['total_complaints'])
            complaints_dict = {k:v for (k,v) in zip_dict.items() if k.isupper() }
            descriptors_dict = {k:v for (k,v) in zip_dict.items() if not k.isupper() and k != 'total_complaints'}
            # comp1 comp2 comp3 represent top three complaints for zip code
            # desc1 desc2 desc3 desc4 desc5 represent top three descriptors of complaints for zip code
            comp1 = ''
            comp2 = ''
            comp3 = ''
            desc1 = ''
            desc2 = ''
            desc3 = ''
            desc4 = ''
            desc5 = ''
            c_dict_len = len(complaints_dict)
            d_dict_len = len(descriptors_dict)
            if c_dict_len < 3 and c_dict_len != 0:
                # Less than three types of complaints, display only the top one
                top_complaint = nlargest(1, complaints_dict.items(), key=lambda x: x[1])
                comp1 = "{}: {}: {}%".format(top_complaint[0][0], top_complaint[0][1], round(top_complaint[0][1]/total_complaints * 100, 2))
            elif c_dict_len != 0:
                # Three or more complaint types, can compute top 3
                top_n_complaints = nlargest(3, complaints_dict.items(), key=lambda x: x[1])
                comp1 = "{}: {}: {}%".format(top_n_complaints[0][0], top_n_complaints[0][1], round(top_n_complaints[0][1]/total_complaints * 100, 2))
                comp2 = "{}: {}: {}%".format(top_n_complaints[1][0], top_n_complaints[1][1], round(top_n_complaints[1][1]/total_complaints * 100 ,2))
                comp3 = "{}: {}: {}%".format(top_n_complaints[2][0], top_n_complaints[2][1], round(top_n_complaints[2][1]/total_complaints * 100 ,2))
            if d_dict_len < 3 and d_dict_len != 0:
                # Less than three types of descriptors, display only the top one
                top_n_descriptors = nlargest(1, descriptors_dict.items(), key=lambda x: x[1])
                desc1 = "{}: {}: {}%".format(top_n_descriptors[0][0], top_n_descriptors[0][1], round(top_n_descriptors[0][1]/total_complaints * 100, 2))
            elif d_dict_len < 5 and d_dict_len != 0:
                # Between 3 and 4 descriptors, compute top 3
                top_n_descriptors = nlargest(3, descriptors_dict.items(), key=lambda x: x[1])
                desc1 = "{}: {}: {}%".format(top_n_descriptors[0][0], top_n_descriptors[0][1], round(top_n_descriptors[0][1]/total_complaints * 100, 2))
                desc2 = "{}: {}: {}%".format(top_n_descriptors[1][0], top_n_descriptors[1][1], round(top_n_descriptors[1][1]/total_complaints * 100, 2))
                desc3 = "{}: {}: {}%".format(top_n_descriptors[2][0], top_n_descriptors[2][1], round(top_n_descriptors[2][1]/total_complaints * 100, 2))
            elif d_dict_len != 0:
                # 5 or more descriptors, compute top 5
                top_n_descriptors = nlargest(5, descriptors_dict.items(), key=lambda x: x[1])
                desc1 = "{}: {}: {}%".format(top_n_descriptors[0][0], top_n_descriptors[0][1], round(top_n_descriptors[0][1]/total_complaints * 100, 2))
                desc2 = "{}: {}: {}%".format(top_n_descriptors[1][0], top_n_descriptors[1][1], round(top_n_descriptors[1][1]/total_complaints * 100, 2))
                desc3 = "{}: {}: {}%".format(top_n_descriptors[2][0], top_n_descriptors[2][1], round(top_n_descriptors[2][1]/total_complaints * 100, 2))
                desc4 = "{}: {}: {}%".format(top_n_descriptors[3][0], top_n_descriptors[3][1], round(top_n_descriptors[3][1]/total_complaints * 100, 2))
                desc5 = "{}: {}: {}%".format(top_n_descriptors[4][0], top_n_descriptors[4][1], round(top_n_descriptors[4][1]/total_complaints * 100, 2))
            yield (zip_code, (int(total_complaints), comp1, comp2, comp3, desc1, desc2, desc3, desc4, desc5))

    def combine(agg_dict, record_dict):
        for key, value in record_dict.items():
            agg_dict[key] = agg_dict.get(key, 0) + value
        return agg_dict

    aggregate_dict = zip_complaints_rdd.map(lambda x: x[1]) \
                                       .fold({}, combine)
    totals = sorted(aggregate_dict.items(), key=lambda x: x[1], reverse=True)

    three_one_one_statistics_rdd = zip_complaints_rdd.mapPartitions(topn)
    three_one_one_statistics_rdd.coalesce(1).saveAsTextFile("./Results/three_one_one_results")
    sc.parallelize(totals).coalesce(1).saveAsTextFile("./Results/three_one_one_totals")
    return three_one_one_statistics_rdd
