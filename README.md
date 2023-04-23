# Landing Zone

<p align="center"><img src="./pipeline.png" alt="Pipeline image" title="Pipeline image"/></p>
<p align="center"><em>Pipeline diagram we implemented</em></p>

## Temporal Landing Zone
The pipeline our data follows when is added into the temporal landing zone is the following one:

Data Collectors:

1. We manually download the Idealista, OpenData BCN and lookup files into our local machine. You can find all the files in [data](data) folder.
In this case, we assume saving them locally is not a problem due to the small number of files. In case we had huge amounts of data, we should 
send the data directly from the source to HDFS as we do in step 2.


2. We automatically collect data about immigration from the API of OpenData BCN. This data comes from 
[here](https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/). The data presented provides
information on the immigration rate by neighborhood in Barcelona each year, with new reports added annually. These data 
were selected for potential analyses related to rental prices based on the neighborhood and the level of immigration 
typically observed in those areas.

   * For each year we have a different API request, so we automatically scrap all available URLs and get all JSON files 
   for the different available years.

   * As a special case, data from 2018 is not available to get from the API, so we download it manually in a CSV file 
   (i.e., [2018_taxa_immigracio.csv](data%2FotherFiles%2F2018_taxa_immigracio.csv)).


3. We have defined an avro schema for each data topology we had in [resources](resources) folder: one for Idealista 
property JSON files (i.e., [idealista.avsc](resources%2Fidealista.avsc)), one for lookup CSV files 
(i.e., [lookup_tables.avsc](resources%2Flookup_tables.avsc)), another for OpenData Income CSV files 
(i.e., [opendatabcn-income.avsc](resources%2Fopendatabcn-income.avsc)) and another for the OpenData Immigration JSON 
files from the API (i.e., [opendatabcn-immigration.avsc](resources%2Fopendatabcn-immigration.avsc)).

    **_NOTE_**: we did not define a schema for the special 2018 case in the API data collection. 


4. Each raw file (i.e., those in [data](data)) is read and transformed into avro format, if possible (i.e., if it exists 
a predefined schema for them).
   * In case we want to include other files with no schema or unknown schema (e.g., images, files from other sources, files 
   with other formats, special 2018 case, etc.), we load them rawly in our temporal landing Zone. We do not convert files
   to Avro format for which we don't have a predefined schema.

   **_NOTE_**: We decided to convert files to avro format because we assume we could have much more data, so we wanted to 
   compress our files a bit and make them easier to distribute by taking advantage of its horizontal fragmentation. As 
we wanted to use the whole files, we chose Avro instead of Parquet, so in this case it is a better choice in terms of 
performance. Even this conversion has a cost in insertion (i.e., checking the file matches the schema and validating the
presence and the domains of the data) and a cost in preparing the data (i.e., predefining the schemas of the files) 
having the schema of the files could benefit query time by saving an explicit casting and type conversion in future 
analytical steps.


5. Files are sent to HDFS with a given name. HDFS host and port can be found in [config.cfg](src%2Futils%2Fconfig.cfg).
We give a name to the different files in order to recognize where they come
from (e.g., idealista), its original data format (e.g., CSV), its original filename (e.g., 2020_01_02_idealista) and be
able to save different versions of the files by adding their modification timestamp (e.g., 2021-03-09 13:42:45).
   * The names of the local files are given in the following format:
     * In case they are converted to avro format: `source%rawFileFormat%orginalFilename%modificationDate%modifcationTime.avro`
     
         **_NOTE_**: We add the `.avro` extension to identify Avro files and add the original file type inside the filename.
       `source` name is given by the folder where files come from (e.g., [idealista](data%2Fidealista), [lookup_tables](data%2Flookup_tables),
     [opendatabcn-income](data%2Fopendatabcn-income), etc.)

     * In case they are loaded as raw files: `source%orginalFilename%modificationDate%modifcationTime.fileFormat`
     
       **_NOTE_**: We keep the original file extension to identify the formats of the flies at the end of the filename 
     and not inside.

      * In case they are loaded directly from the API:
         * If converted to avro: `source%json%originalFilename.avro`
         * If not converted to avro format: `source%originalFilename.json`
        
         **_NOTE_**: It follows the same structure as the previous cases but we do not include the modification timestamp.
     We should save the files locally to get the modification time so we decide to not include it. This is not a problem 
     since those files are only uploaded on the OpenData BCN repository once a year. Also the original filenames are 
     extracted while scrapping the OpenData BCN website.


6. Each time you run the code, only new data is loaded into HDFS.
   * If a file has not been modified from the last data load, it will not be overwritten in HDFS. This is the reason we 
   added a modification timestamp to the filenames, so every time a file is updated it can be sent to HDFS. Otherwise,
   the modification timestamp will not change and the file will not be included in the temporal Landing Zone because 
   the file already exists in there. This has the same utility as it would have a logs file to check the files that 
   should be included or not and allow us to have different versions of a file in our distributed file system. In this 
   case, taking into account the smallest time granularity is the second, if the same file is modified at the same time 
   (i.e., at the same second) in different servers, only the first version to be loaded in HDFS will be kept.

## Persistent Landing Zone
#### Main structure
- The data would be stored in one big table called `datalake`, and would store each file as a table entry (row)
- Justification
  - Because of the flexibility that HBase gives, there was no need to have more than one table, and also all of the files would be easily accessible because of how the key is defined
#### Key design:
- Format: 
  `source$fileFormat$orginalFilename$modificationTimeStamp`
- Example: `opendatabcn-income$csv$2016_Distribucio_territorial_renda_familiar$2021-03-10T09-54-21`
- Justification:
  - It was needed to track the ingested files easily by their data source, so it is specified, the version of the file, that can be done using the filename and by the different modification times the different versions can be checked, and we also considered important to be able to track by the file format to maybe in the next zones separate files by this criteria, this can be useful for example to apply different preprocessing to each file type
  - Modification time was considered as an alternative of registering the loading time to avoid the creation of a hotspot in this region

#### Families structure:
- There are two column families defined:
  - `data`: contains the file
  - `metadata`: contains the file information, in this case it would contain the file schema

#### Data Persistent Loader:
The pipeline it follows is:

1. Connects to the HDFS server, using the hdfs python library, and sets the source path route as  `/user/bdm/Temporal_LZ`, so everything in that file would be loaded
2. Connects to the HBase server, using the happybase python library

    **_NOTE:_** to be able to connect to the HBase server it was needed to have the database started:
    ```shell 
   /BDM_Software/hbase/bin/start-hbase.sh 
   ```
    And then execute the following command, to start the thrift server daemon and be able to connect to it:
    ```shell
   /hbase-daemon.sh start thrift
   ```
3. In the HBase connection it checks if the table with the name defined in the `config.cfg` file (in this case `datalake`) exists, and if not, it creates it, defining two column families, `data` and `metadata`.
4. The Data Persistent Loader reads each file stored on the temporal zone, to do this first it analyzes the file extension and it has two choices:

   1. For Avro files:
      1. Generates the key using the file name: `source$fileFormat$orginalFilename$modificationTimeStamp```
      2. Read the avro file, creates a dictionary for the entry value, where it saves the file content as a list of jsons and the file schema

            **_NOTES:_**  
         - The file content is saved as a list of jsons to facilitate its encoding to be able to be inserted into the HBase table
         - The key of each part of the value content has to contain the column family where it belongs, for the file content it has to be in the data cf, so it would be `data:file`, and for the schema it would be `metadata:schema`, leaving the json like this:
         ```json lines
             {"data:file" : {...}, "metadata:schema": {...}}
         ```

      3. Creates a dictionary, using the generated key and the dictionary of the entry value, and inserts it into the HBase table
      
            **_NOTE:_** Each entry of the HBase table corresponds to a file, and would look like this:
            ```json lines
            {"source$fileFormat$orginalFilename$modificationTimeStamp" : {"data:file" : {...}, "metadata:schema": {...}} }
         ```
   2. For raw files (any other extension different from avro):
      1. Generates the key using the file name: `source$fileFormat$orginalFilename$modificationTimeStamp`
      2. Creates a dictionary for the entry value, opens the file contents and saves it with no modification in the dictionary
         
           **_NOTES:_** 
         
         - Thinking in a real life case, there is no method that guarantees a correct extraction of the schema from any file, since sometimes this will not be explicit in the file, so for the raw files it would not be saved, leaving the json like this:
         ```json lines
             {"data:file" : {...} }
         ```
         - Here it can be seen the usefulness of the key-value storage because of its versatility, the two types of entries seen would be inserted to the same table
      3. Creates a dictionary, using the generated key and the dictionary of the entry value, and inserts it into the HBase table

         **_NOTE:_** Each entry of the HBase table corresponds to a file, and would look like this:
         ```json lines
            {"source$fileFormat$orginalFilename$modificationTimeStamp" : {"data:file" : {...}} }
         ```
5. Everytime this pipeline is executed new data would be inserted and the repeated one would be overwritten, because the HBase system would check it using the generated key, and as we use modification time and no loading time there would not be duplicated data, but version control is handled

    **_NOTE:_**  Also in a real life situation the Temporal Landing would be purged from time to time, so there would not be many cases that the data could be repeated


# How to run the code
Before starting the loading of the data, is important to first start HDFS in our local machine:
```{bash}
/home/bdm/BDM_Software/hadoop/sbin/start-dfs.sh
```
The Temporal Landing Zone directory where we work is going to be on the `/user/bdm` folder in HDFS, so first of all we need to create it:
```{bash}
~/BDM_Software/hadoop/bin/hdfs dfs -mkdir /user
~/BDM_Software/hadoop/bin/hdfs dfs -mkdir /user/bdm 
~/BDM_Software/hadoop/bin/hdfs dfs -chmod -R 777 /user/bdm/
```
To load both the temporal and persistent landing zones with the data, you just need to run [run.sh](run.sh).
Simply running it, the full pipeline is implemented. You could see that temporal landing zone files will be stored at `/user/bdm/Temporal_LZ` directory.
This script basically runs [temporalLanding.py](temporalLanding.py) which is in charge of loading the temporal landing zone given some arguments in the console, and 
[persistentLanding.py](persistentLanding.py), which loads data in the persistent landing zone. 
The script tries to upload preferable Avro files in HDFS. In case it is not possible due to any reason (mainly because of a lack of a predefined schema), it loads the files in raw format.
    
### Configurations:
The project contains a [config.cfg](src%2Futils%2Fconfig.cfg) file, where important constants are set:
- The VM server information
- The path of the zones
- Configuration names, such as table names

**_NOTE:_** If you need to install some modules you can run the `pip install -r requirements.txt` command on top of [run.sh](run.sh) script. 
If you do not want to load Avro files but only the raw data, you can uncomment `UPLOAD RAW FILES TO HDFS` section in [run.sh](run.sh) and comment the `CREATE AND UPLOAD AVRO FILES` section.

Here we show some instructions on how to run the different files in case it is preferred to run it in a different way.
## How to run [temporalLanding.py](temporalLanding.py)
We have different options to load the data in HDFS. Before using them, remember to set properly the environment variables as in [run.sh](run.sh):
```{bash}
export PROJECT_DIRECTORY="$PWD"
export HDFS_DIRECTORY="/user/bdm/Temporal_LZ/"
```
### Options to load Avro format files (if possible)
1. Load all data in HDFS. This option loads all local files in [data](data) and loads the data collected from the
[OpenData BCN API](https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/). 
If the files have a predefined schema in [resources](resources), they will be loaded in Avro format. 
Otherwise, they will be loaded in raw format. For example, when we want to load other files we could have from 
[otherFiles](data%2FotherFiles) folder such as the previously mentioned [special 2018 case](data%2FotherFiles%2F2018_taxa_immigracio.csv) 
from the API, those files will be uploaded with no conversion. In our case we have the 2018 immigration `.csv` file from OpenData BCN API and a `.jpeg` image of some house in Barcelona.

   **_NOTE:_** Schema file names need to be the same as the data folder where files are contained. Otherwise, schemas will not be assigned to the data.
If we need to add new data from the same source but with a different schema, we will need to create a new folder with a new name (same as it would have the new schema) to save the new data.

```{bash}
python temporalLanding.py write avro
```
2. Load specific local files. If you want to load only specific local data folders, you can do it by specifying the name of the folder your files are contained in.
Same logic as in previous option is applied here.
```{bash}
python temporalLanding.py write avro -i idealista
```
```{bash}
python temporalLanding.py write avro -i lookup_tables
```
```{bash}
python temporalLanding.py write avro -i opendatabcn-income
```
```{bash}
python temporalLanding.py write avro -i otherFiles
```
3. Load only data from the [API](https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/). Same logic is applied here.
```{bash}
python temporalLanding.py write avro -i opendatabcn-immigration
```
### Options to load raw format files
1. Load all data in HDFS. This option loads all local files in [data](data) and loads the data collected from the
[OpenData BCN API](https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/).
Using this option, files are loaded in raw format (e.g., CSV, JSON, etc.)

```{bash}
python temporalLanding.py write raw
```
2. Load specific local files. If you want to load only specific local data folders, you can do it by specifying the name of the folder your files are contained in.
```{bash}
python temporalLanding.py write raw -i idealista
```
```{bash}
python temporalLanding.py write raw -i lookup_tables
```
```{bash}
python temporalLanding.py write raw -i opendatabcn-income
```
```{bash}
python temporalLanding.py write raw -i otherFiles
```
3. Load only data from the [API](https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/). 
```{bash}
python temporalLanding.py write raw -i opendatabcn-immigration
```
**_NOTE_**: Files from [reader](src%2Freader) are not used in our pipeline, they are simply readers for Avro and Parquet files (in case you have them saved locally) that have been useful to work with.
They can be used as:
```{bash}
python temporalLanding.py read avro -r <localAvroFilePath>
```
```{bash}
python temporalLanding.py read parquet -r <localParquetFilePath>
```
## How to run [persistentLanding.py](persistentLanding.py)
The Data Persistent Loader code reads each file stored on the temporal zone, on the defined route, and loads it all to 
the persistent landing zone, so there are no major settings to do, so the file does not require parameters, and can 
be executed like this:

```{bash}
python persistentLanding.py
```

