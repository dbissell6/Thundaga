# Thundaga

Thundaga is forensic analysis tool designed to efficiently parse and visualize AWS CloudTrail logs. 

Humans invested lots of evolutionary points into vision, let's use it.

![Screenshot 2023-12-28 190939](https://github.com/dbissell6/Thundaga/assets/50979196/6b068dc4-65e0-411e-b33a-6f6bde662edb)

# Install

![image](https://github.com/dbissell6/Thundaga/assets/50979196/c1db0a55-3a8f-4a9f-b065-d589d770228b)


# Use

1) Every use will begin with reading and parsing the data. Converting the json to dataframes based on eventName.

2) stats, analyze, query

3) clean

# Read

![image](https://github.com/dbissell6/Thundaga/assets/50979196/b645dd57-0ca1-4926-af26-c2a2704170a8)

# Get stats

![image](https://github.com/dbissell6/Thundaga/assets/50979196/38cf5f50-f263-4e58-b2f7-4b99612619ce)

# Analyze

## Investigate Ips

![image](https://github.com/dbissell6/Thundaga/assets/50979196/dcbbb0a2-90b6-4b84-b268-ebd8f0fdf9df)

![image](https://github.com/dbissell6/Thundaga/assets/50979196/73bd403a-ea66-4678-8001-eec221f84556)

## Investigate Arn

![image](https://github.com/dbissell6/Thundaga/assets/50979196/2665b48a-422e-46b2-8adb-07a8e601866e)

![image](https://github.com/dbissell6/Thundaga/assets/50979196/e514ad84-148b-4b77-93bc-88f46c363bcd)

## Investigate Bucket

![image](https://github.com/dbissell6/Thundaga/assets/50979196/ef7c8509-adda-44cd-ac05-94c76f03a313)

![image](https://github.com/dbissell6/Thundaga/assets/50979196/c8002a22-5a46-40a2-8efb-c3c862aba0f3)

## anon

![image](https://github.com/dbissell6/Thundaga/assets/50979196/e1770f02-005b-4485-8586-c84ab14a69ef)

![image](https://github.com/dbissell6/Thundaga/assets/50979196/96ce74c8-28ef-4458-9549-745e63c7211c)

## Misc

### Exclude IPs

Some plots allow excluding IPs

![image](https://github.com/dbissell6/Thundaga/assets/50979196/a08758af-cf12-4e06-92ee-5f77c29d27c0)



# query

## strings

First parameter is eventName(if want all choose ALL), second parameter is the case sensitive string to search for. Output to query_output.txt.

![image](https://github.com/dbissell6/Thundaga/assets/50979196/31c77534-1f5d-454c-9c8c-1688a06353f7)



# Clean

Remove files created during analysis

![image](https://github.com/dbissell6/Thundaga/assets/50979196/d910f1f9-9026-4f47-9d0c-36f20ea28d17)

