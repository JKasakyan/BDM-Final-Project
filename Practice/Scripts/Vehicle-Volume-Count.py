def main(sc, dataset):
    volume_rdd = sc.textFile(dataset, use_unicode=False)
    header = volume_rdd.first()

    def format_street_name(name):
        """
        Some records are of form "111 ST" or "WEST 14 ST". This causes problems with geopy's Nominatim geocoder, which
        expects "111th ST" or "WEST 14th ST"
        """
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
            roadway = fields[2].strip()
            from_address = fields[3].strip()
            to_address = fields[4].strip()
            num_vehicles = 0

            roadway = format_street_name(fields[2].strip())
            from_address = format_street_name(fields[3].strip())
            to_address = format_street_name(fields[4].strip())

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

    from geopy.geocoders import Nominatim

    def geomMap(records):
        geolocator = Nominatim(format_string="%s, NY", country_bias="USA", timeout=2)
        for record in records:
            seg_id = record[0]
            from_address = record[1]['from']
            to_address = record[1]['to']
            roadway = record[1]['roadway']

            roadway_dict = geolocator.geocode(roadway, addressdetails=True).raw
            from_dict    = geolocator.geocode(from_address, addressdetails=True).raw
            to_dict      = geolocator.geocode(to_address, addressdetails=True).raw

            roadway_address = roadway_dict['address']
            from_address    = from_dict['address']
            to_address      = to_dict['address']

            roadway_bounding_box = roadway_dict['boundingbox']
            from_bounding_box    = from_dict['boundingbox']
            to_bounding_box      = to_dict['boundingbox']

            yield (seg_id, (roadway, from_address, to_address, roadway_dict, from_dict, to_dict))


    geom_detail_rdd = seg_group_volume_rdd.mapPartitions(geomMap)
    geom_detail_rdd.take(2).parallelize.coalesce(1).saveAsTextFile('Temp')

if __name__ == "__main__":
    sc = pyspark.SparkContext()
    arguments = sys.argv
    dataset = arguments[1]
    main(sc, dataset)
