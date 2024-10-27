import clickhouse_connect
import pandas as pd



def create_tables(client):
    # Create the customers table
    client.command('''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id Int32,
        first_name String,
        last_name String,
        email String,
        phone String,
        age Int32,
        city String
    )
    ENGINE = MergeTree()
    ORDER BY customer_id
    ''')
    
    # Create the travels table
    client.command('''
    CREATE TABLE IF NOT EXISTS travels (
        destination_id Int32,
        country String,
        city String,
        attraction String,
        best_season Enum8('Spring' = 1, 'Summer' = 2, 'Autumn' = 3, 'Winter' = 4),
        average_cost Float32,
        rating Float32
    )
    ENGINE = MergeTree()
    ORDER BY destination_id
    ''')

def insert_data(client):
    # Add sample data
    customer_data = [
    (1, 'Alice', 'Koshkarbay', 'alice_k@gmail.com', '87055325932', 30, 'Almaty'),
    (2, 'Bob', 'Koshken', 'bob_k@gmail.com', '87775425932', 37, 'Astana'),
    (3, 'Ilyas', 'Aldas', 'ilyas_a@gmail.com', '87015226421', 20, 'Almaty'),
    (4, 'Dana', 'Baizhan', 'dana_b@gmail.com', '87772325930', 27, 'Almaty'),
    (5, 'Erik', 'Suleimen', 'erik_s@gmail.com', '87093215678', 32, 'Astana'),
    (6, 'Mira', 'Zhanturin', 'mira_z@gmail.com', '87755437821', 25, 'Shymkent'),
    (7, 'Talgat', 'Zhaxybek', 'talgat_j@gmail.com', '87093457823', 29, 'Almaty'),
    (8, 'Aigerim', 'Mukhamedzhan', 'aigerim_m@gmail.com', '87781231234', 26, 'Astana'),
    (9, 'Dauren', 'Nurtai', 'dauren_n@gmail.com', '87099451230', 33, 'Shymkent'),
    (10, 'Nursultan', 'Zhansaya', 'nursultan_z@gmail.com', '87034423125', 28, 'Almaty'),
    (11, 'Zhansaya', 'Ospanova', 'zhansaya_o@gmail.com', '87775431244', 22, 'Astana'),
    (12, 'Arman', 'Beketov', 'arman_b@gmail.com', '87056982145', 34, 'Shymkent'),
    (13, 'Saule', 'Nurkyzy', 'saule_n@gmail.com', '87785542168', 28, 'Almaty'),
    (14, 'Rustem', 'Nurtas', 'rustem_n@gmail.com', '87031234567', 31, 'Astana'),
    (15, 'Gulnara', 'Kazhibek', 'gulnara_k@gmail.com', '87791239876', 27, 'Shymkent'),
    (16,  'Bolat', 'Zharkenov', 'bolat_z@gmail.com', '87019872345', 29, 'Almaty'),
    (17,  'Aida', 'Sagatova', 'aida_s@gmail.com', '87065321458', 24, 'Astana'),
    (18,  'Azamat', 'Syzdykov', 'azamat_s@gmail.com', '87786234509', 35, 'Almaty'),
    (19,  'Samal', 'Amanzhol', 'samal_a@gmail.com', '87034981234', 28, 'Almaty'),
    (20, 'Rinat', 'Bazarbay', 'rinat_b@gmail.com', '87793123455', 30, 'Astana'),
    (21,  'Asel', 'Bolat', 'asel_b@gmail.com', '87091234501', 31, 'Almaty'),
    (22,  'Rasul', 'Kadir', 'rasul_k@gmail.com', '87775551234', 34, 'Almaty'),
    (23,  'Malika', 'Tazhibay', 'malika_t@gmail.com', '87035674321', 26, 'Astana'),
    (24,  'Kairat', 'Serik', 'kairat_s@gmail.com', '87791235476', 35, 'Shymkent'),
    (25,  'Zarina', 'Omarova', 'zarina_o@gmail.com', '87036662345', 27, 'Almaty'),
    (26,  'Samat', 'Issatay', 'samat_i@gmail.com', '87781237891', 32, 'Astana'),
    (27,  'Aliya', 'Yerlan', 'aliya_y@gmail.com', '87035421678', 29, 'Shymkent'),
    (28,  'Ruslan', 'Nurly', 'ruslan_n@gmail.com', '87775431990', 28, 'Almaty'),
    (29,  'Dina', 'Samat', 'dina_s@gmail.com', '87036549876', 23, 'Astana'),
    (30,  'Bauyrzhan', 'Daniyar', 'bauyrzhan_d@gmail.com', '87783451234', 36, 'Shymkent'),
    (31,  'Saltanat', 'Zhakypova', 'saltanat_z@gmail.com', '87035423198', 24, 'Almaty'),
    (32,  'Zhanna', 'Bek', 'zhanna_b@gmail.com', '87792345678', 29, 'Astana'),
    (33,  'Askar', 'Akzhol', 'askar_a@gmail.com', '87095431234', 34, 'Shymkent'),
    (34,  'Moldir', 'Kadirova', 'moldir_k@gmail.com', '87783456912', 30, 'Almaty'),
    (35,  'Yernar', 'Aslan', 'yernar_a@gmail.com', '87035428734', 27, 'Astana'),
    (36,  'Gulnur', 'Serikbay', 'gulnur_s@gmail.com', '87783451276', 26, 'Almaty'),
    (37,  'Timur', 'Almas', 'timur_a@gmail.com', '87091235412', 33, 'Almaty'),
    (38,  'Rayana', 'Aldiyar', 'rayana_a@gmail.com', '87775546789', 25, 'Astana'),
    (39,  'Yelaman', 'Kairat', 'yelaman_k@gmail.com', '87093451234', 40, 'Almaty'),
    (40,  'Aruzhan', 'Nursultan', 'aruzhan_n@gmail.com', '87792345678', 24, 'Almaty')
    ]
    
    travel_data = [
    (1, 'Japan', 'Tokyo', 'Mount Fuji', 'Spring', 1200.50, 4.9),
    (2, 'France', 'Paris', 'Eiffel Tower', 'Autumn', 1800.75, 4.8),
    (3, 'Italy', 'Rome', 'Colosseum', 'Summer', 1600.60, 4.7),
    (4, 'USA', 'New York', 'Statue of Liberty', 'Spring', 2200.80, 4.6),
    (5, 'Australia', 'Sydney', 'Sydney Opera House', 'Summer', 2000.50, 4.8),
    (6, 'Brazil', 'Rio de Janeiro', 'Christ the Redeemer', 'Spring', 1500.30, 4.7),
    (7, 'India', 'Agra', 'Taj Mahal', 'Winter', 1400.25, 4.9),
    (8, 'China', 'Beijing', 'Great Wall', 'Autumn', 1300.60, 4.8),
    (9, 'Egypt', 'Cairo', 'Pyramids of Giza', 'Winter', 1700.45, 4.7),
    (10, 'Peru', 'Cusco', 'Machu Picchu', 'Summer', 1900.90, 4.9),
    (11, 'Greece', 'Athens', 'Acropolis', 'Spring', 1600.75, 4.6),
    (12, 'South Africa', 'Cape Town', 'Table Mountain', 'Autumn', 1100.30, 4.7),
    (13, 'Thailand', 'Bangkok', 'Grand Palace', 'Winter', 1300.15, 4.5),
    (14, 'Turkey', 'Istanbul', 'Hagia Sophia', 'Spring', 1500.55, 4.6),
    (15, 'Russia', 'Moscow', 'Red Square', 'Winter', 1800.95, 4.5),
    (16, 'Canada', 'Banff', 'Banff National Park', 'Summer', 2100.25, 4.8),
    (17, 'Mexico', 'Mexico City', 'Chichen Itza', 'Autumn', 1600.50, 4.7),
    (18, 'Spain', 'Barcelona', 'Sagrada Familia', 'Summer', 1700.60, 4.8),
    (19, 'UK', 'London', 'Big Ben', 'Spring', 2200.85, 4.6),
    (20, 'Argentina', 'Buenos Aires', 'Iguazu Falls', 'Autumn', 1500.45, 4.7)
    ]

    client.insert('customers', customer_data, column_names=['customer_id', 'first_name', 'last_name', 'email', 'phone', 'age', 'city'])
    client.insert('travels', travel_data, column_names=['destination_id', 'country', 'city', 'attraction', 'best_season', 'average_cost', 'rating'])


# Connect to the Clickhouse server 

client = clickhouse_connect.get_client(host='localhost', port=8123)

create_tables(client)
insert_data(client)

cust_query_result = client.query('select * from customers')
travel_query_result = client.query('select * from travels')

cust_df = pd.DataFrame(cust_query_result.result_rows, columns=cust_query_result.column_names)
travel_df = pd.DataFrame(travel_query_result.result_rows, columns=travel_query_result.column_names)

if __name__ == "__main__":
    print('Ok')
