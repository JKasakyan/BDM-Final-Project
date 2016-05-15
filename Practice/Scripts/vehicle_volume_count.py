def get_rdd(sc, count_csv_path):
    """
    sc is SparkContext
    count_csv_path is relative path to location of csv produced by running
    generate_vehicle_count_csv.py
    """
    zip_num_vehicles_rdd = sc.textFile(count_csv_path)
    header = zip_num_vehicles_rdd.first()
    def map_multiple_zip(records):
        """
        Some results are of form 11220;11204,39294 or 10309: 10312. Need to count as two separate records. One record is NY 10026,12496, duplicate
        of record before it
        """
        for record in records:
            if not record == header:
                fields = record.split(',')
                zip_code = fields[0]
                vehicle_count = int(fields[1])
                if "NY" in zip_code:
                    # Duplicate record with incorrect format
                    continue
                elif ';' in zip_code:
                    # Of form 11220;11204,39294
                    zip1 = zip_code.split(';')[0].strip()
                    zip2 = zip_code.split(';')[1].strip()
                    half_count = vehicle_count / 2
                    yield (zip1, (half_count, 1))
                    yield(zip2, (half_count, 1))
                elif ':' in zip_code:
                    # Of form 10309: 10312
                    zip1 = zip_code.split(':')[0].strip()
                    zip2 = zip_code.split(':')[1].strip()
                    half_count = vehicle_count / 2
                    yield (zip1, (half_count, 1))
                    yield(zip2, (half_count, 1))
                else:
                    yield (zip_code, (vehicle_count, 1))

    zip_key_rdd = zip_num_vehicles_rdd.mapPartitions(map_multiple_zip)

    def myReducer(count, tup):
        print ("count = {}\n".format(count))
        print ("tup = {}\n".format(tup))
        return count + tup[0]

    def seqOp(count, tup):
        return count + tup[0]

    def combOp(count1, count2):
        return count1 + count2

    result_rdd = zip_key_rdd.aggregateByKey(0, seqOp, combOp)
    result_rdd.coalesce(1).saveAsTextFile("./Results/vehicle_volume_results")
    return result_rdd
