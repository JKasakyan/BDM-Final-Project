"""
This script outputs a csv that contains two columns, a zip code and number of accidents.
Can not be run on cluster due to restrictions on geopy.geocoders.Nominatim.
"""


def main(sc, input_location, output_location):
    """
    input_location is relative path to NYC OpenData's Traffic_Volume_Counts__2012-2013.csv
    output_location is relative path for directory where output csv will be saved
    """
    volume_rdd = sc.textFile(input_location, use_unicode=False).cache()
    header = volume_rdd.first()

    def format_street_name(name):
        """
        Some records are of form "111 ST" or "WEST 14 ST". This causes problems with geopy's Nominatim geocoder, which
        expects "111th ST" or "WEST 14th ST"
        """
        if name == "AVE OF THE AMER":
            return "AVE OF THE AMERICAS"
        number_dict = {'1': 'st', '2': 'nd', '3': 'rd', '11': 'th', '12': 'th', '13': 'th', '14': 'th', '15': 'th', '16': 'th', '17': 'th', '18': 'th', '19': 'th'}
        words = name.split(' ')
        new_string = ""
        for word in words:
            word = word.strip()
            if word.isdigit():
                if len(word) == 1:
                    # Single digit number
                    new_string += word + number_dict.get(word, 'th') + " "

                else:
                    # At least two digit number
                    last_digit = word[-1]
                    last_two_digits = word[-2:]
                    new_string += word + number_dict.get(last_two_digits, number_dict.get(last_digit, 'th')) + " "
            else:
                new_string += word + " "
        return new_string

    def mapSegmentID(records):
        for record in records:
            if "Segment ID" in record:
                continue
            fields = record.split(',')
            seg_id = fields[1]
            num_vehicles = 0

            roadway = format_street_name(fields[2].strip()).strip()
            from_address = format_street_name(fields[3].strip()).strip()
            to_address = format_street_name(fields[4].strip()).strip()

            for i in range (7, 31):
                try:
                    count = int(fields[i])
                    num_vehicles += count
                except ValueError:
                    continue
            yield (seg_id, (roadway, from_address, to_address, num_vehicles))

    volume_rdd_seg = volume_rdd.mapPartitions(mapSegmentID)

    def seqOp(seg_dict, tup):
        roadway = tup[0]
        from_address = tup[1]
        to_address = tup[2]
        vehicle_count = tup[3]
        seg_dict['roadway'] = seg_dict.get('roadway', roadway)
        seg_dict['from'] = seg_dict.get('from', from_address)
        seg_dict['to'] = seg_dict.get('to', to_address)
        seg_dict['vehicle_count'] = seg_dict.get('vehicle_count', 0) + vehicle_count

        return seg_dict

    def combOp(seg_dict1, seg_dict2):
        seg_dict2['vehicle_count'] += seg_dict1.get('vehicle_count', 0)
        return seg_dict2

    seg_group_volume_rdd = volume_rdd_seg.aggregateByKey({}, seqOp, combOp)
    seg_group_volume_rdd.collect()

    from geopy.geocoders import Nominatim

    def geomMap(records):
        geolocator = Nominatim(format_string="%s, NY", country_bias="USA", timeout=10)
        for record in records:
            print record
            print ("______________")
            seg_id = record[0]
            from_address = record[1]['from'].strip()
            to_address = record[1]['to'].strip()
            roadway = record[1]['roadway'].strip()
            vehicle_count = record[1]['vehicle_count']

            try:
                roadway_dict = geolocator.geocode(roadway, addressdetails=True).raw
                from_dict    = geolocator.geocode(from_address, addressdetails=True).raw
                to_dict      = geolocator.geocode(to_address, addressdetails=True).raw
            except AttributeError:
                # Couldn't geolocate, skip this record
                print ("SKIPPING {}".format(record))
                print("__________")
                continue

            try:
                roadway_address = roadway_dict['address']
                from_address    = from_dict['address']
                to_address      = to_dict['address']
            except:
                # No address field, skip this record
                print ("SKIPPING {}".format(record))
                print("__________")
                continue

            try:
                roadway_postcode = roadway_address['postcode'].strip()
                from_postcode = from_address['postcode'].strip()
                to_postcode = to_address['postcode'].strip()
            except:
                # No postcode field, skip this record
                print ("SKIPPING {}".format(record))
                print("__________")
                continue

            from_postcode_count = vehicle_count
            to_postcode_count = vehicle_count
            half_vehicle_count = vehicle_count / 2
            one_third_vehicle_count = vehicle_count / 3
            two_third_vehicle_count = 2 * (vehicle_count / 3)

            if from_postcode != to_postcode:
                if roadway_postcode == from_postcode:
                    from_postcode_count = two_third_vehicle_count
                    to_postcode_count = one_third_vehicle_count

                    yield(from_postcode, from_postcode_count)
                    yield(to_postcode, to_postcode_count)
                elif roadway_postcode == to_postcode:
                    to_postcode_count = two_third_vehicle_count
                    from_postcode_count = one_third_vehicle_count

                    yield(from_postcode, from_postcode_count)
                    yield(to_postcode, to_postcode_count)
                else:
                    to_postcode_count = one_third_vehicle_count
                    from_postcode_count = one_third_vehicle_count
                    roadway_postcode_count = one_third_vehicle_count

                    yield(from_postcode, from_postcode_count)
                    yield(to_postcode, to_postcode_count)
                    yield(roadway_postcode, roadway_postcode_count)
            else:
                # From and to postcode's agree
                if roadway_postcode == from_postcode:
                    # All three agree
                    yield(roadway_postcode, vehicle_count)
                else:
                    # Only from and to agree
                    yield(from_postcode, two_third_vehicle_count)
                    yield(roadway_postcode, one_third_vehicle_count)

    def toCSVLine(data):
        return ','.join(str(d) for d in data)

    seg_group_volume_rdd.mapPartitions(geomMap).map(toCSVLine).coalesce(1).saveAsTextFile(output_location)

if __name__ == "__main__":
    sc = pyspark.SparkContext()
    main(sc, "../Datasets/Traffic_Volume_Counts__2012-2013.csv", "./Results/volume_results")
