add JARS file:///home/local/zslf023/hivexmlserde-1.0.5.1.jar;

-- first test, extatom file, just loading id attribute

CREATE EXTERNAL TABLE idsonly(pdbid STRING)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/hive"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);

select split(pdbid,'-')[0] from xml_pdb;

-- creating from extatom file
-- problem that creates an array of strings. is there a way to break up the array into a struct?

CREATE EXTERNAL TABLE ids_atomstring(pdbid STRING, record ARRAY<string>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName",
"column.xpath.record"="/*[local-name()='datablock']/*[local-name()='category_atom_record']/*[local-name()='atom_record']/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/hive"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);

select pdbid, record[0] from ids_atomstring;
-- returns
-- 11AS-extatom	ATOM    1   A    A      4      4  ?  .  ALA  ALA   N  N  N    11.746    37.328    28.300    1.00   35.74     1    ?

select pdbid,split(record[0],"\\p{Zs}+") from ids_atomstring;
-- 11AS-extatom	["ATOM","1","A","A","4","4","?",".","ALA","ALA","N","N","N","11.746","37.328","28.300","1.00","35.74","1","?"]


-- row for every atom record entry with its corresponding pdb id
SELECT pdbid, entry
FROM ids_atomstring LATERAL VIEW explode(record) adTable AS entry;

--explode and split arrays, one array for each
SELECT split(pdbid,"-")[0], split(entry,"\\p{Zs}+")
FROM ids_atomstring LATERAL VIEW explode(record) splitTable AS entry;
--returns for all items in the array
--11BA	["ATOM","1","B","B","27","27","?",".","ASN","ASN","O","OD1","OD1","-8.638","-9.074","13.089","1.00","19.60","1","?"]

-- create split up table
CREATE TABLE splitPDB
   AS
SELECT split(pdbid,"-")[0] as pdbid, split(entry,"\\p{Zs}+") as atom
FROM ids_atomstring LATERAL VIEW explode(record) splitTable AS entry;

describe splitpdb;


--selecting only residue name and coordinates
select pdbid, record[8],record[13],record[14],record[15]
from splitpdb
where record[0] = "ATOM";






--result of query
11AS	ALA	11.746	37.328	28.300
11AS	ALA	12.364	38.679	28.168
11AS	ALA	13.388	38.646	27.027
11AS	ALA	13.261	37.848	26.096
11AS	ALA	13.027	39.086	29.501
11AS	TYR	14.341	39.569	27.044
11AS	TYR	15.490	39.455	26.171
11AS	TYR	16.675	39.000	27.006
11AS	TYR	17.536	38.259	26.534



-- error checking before creating coordinates table
CREATE TABLE error_records_old
AS
SELECT DISTINCT pdbid FROM splitpdb where size(record) !=20;


CREATE TABLE error_records
AS
SELECT DISTINCT pdbid from splitpdb WHERE
record[0] != "HETATM" 
AND
(size(record) !=20
OR record[2] = "."
OR record[2] = "?"
OR record[4] = "."
OR record[8] = "."
OR record[8] = "?"
OR record[11] = "."
OR record[11] = "?"
OR record[13] = "."
OR record[13] = "?"
OR record[14] = "."
OR record[14] = "?"
OR record[15] = "."
OR record[15] = "?");





--create coordinates table
--0.10 Hive does not support subqueries in where clause gah. introduced in 0.13
CREATE TABLE coordinates
   AS
select pdbid, cast(record[1] as int) as PDB_model_num, record[2] as label_asym_id, cast(record[4] as int) as label_seq_id, record[8] as label_comp_id, record[11] as label_atom_id, cast(record[13] as double) as CartnX,cast(record[14] as double) as CartnY,cast(record[15] as double) as CartnZ
from splitpdb
where record[0] = "ATOM";


11BA	1	B	123	SER	O	7.766	-18.513	23.469
11BA	1	B	123	SER	CB	7.378	-15.44	23.087
11BA	1	B	123	SER	OG	6.147	-16.088	22.754
11BA	1	B	124	VAL	N	9.044	-17.383	24.962
11BA	1	B	124	VAL	CA	9.016	-18.423	25.969
11BA	1	B	124	VAL	C	8.848	-17.901	27.392
11BA	1	B	124	VAL	O	9.346	-16.812	27.72
11BA	1	B	124	VAL	CB	10.281	-19.317	25.999
11BA	1	B	124	VAL	CG1	10.057	-20.488	25.049
11BA	1	B	124	VAL	CG2	11.497	-18.542	25.536
11BA	1	B	124	VAL	OXT	8.218	-18.656	28.167

hive (biotest)> describe coordinates;                             
OK
pdbid	string	
pdb_model_num	int	
label_asym_id	string	
label_seq_id	int	
label_comp_id	string	
label_atom_id	string	
cartnx	double	
cartny	double	
cartnz	double



--get coordinates for a specific pdb id
select * from coordinates where pdbid="11AS";



--try with exclusion of error records
--doesn't work when no rows in the error_records maybe?
--how to make a script that has conditions in it? :/

CREATE TABLE coordinates_exclude AS
select splitpdb.pdbid, cast(record[1] as int) as PDB_model_num, record[2] as label_asym_id, cast(record[4] as int) as label_seq_id, record[8] as label_comp_id, record[11] as label_atom_id, cast(record[13] as double) as CartnX,cast(record[14] as double) as CartnY,cast(record[15] as double) as CartnZ
from splitpdb
LEFT OUTER JOIN error_records
ON (splitpdb.pdbid = error_records.pdbid)
WHERE error_records.pdbid IS null
and record[0] = "ATOM";










--make sure it works with subdirectories
--have to set some environment variables first

set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;  

--then creates the table properly even with subdirectories
--BUT hive bug when trying to split a table with subdirectories... GAH
--possible workaround - create new table and then split? no. doesn't work. 
--possibly have to apply the patch for MAPREDUCE-5756


CREATE EXTERNAL TABLE idsubs(pdbid STRING)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/hivesubs"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);












--ALTERNATIVE EXPERIMENT WITH FULLY MARKED-UP FILES--
-- using the fully marked up file -> array of maps?
-- problem is "Complex content being used as a primitive type 
-- will be converted to a valid XML string by adding a root element called <string>"
-- have to use complex xpath queries to get information. arrays are easier.

CREATE EXTERNAL TABLE ids_maps1(pdbid STRING, record array<map<string,string>> )
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName",
"column.xpath.record"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/hive_all"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);

-- query returns
-- 11AS	{"atom_site":"<string><PDBx:B_iso_or_equiv xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">34.19</PDBx:B_iso_or_equiv><PDBx:Cartn_x xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">12.364</PDBx:Cartn_x><PDBx:Cartn_y xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">38.679</PDBx:Cartn_y><PDBx:Cartn_z xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">28.168</PDBx:Cartn_z><PDBx:auth_asym_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">A</PDBx:auth_asym_id><PDBx:auth_atom_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">CA</PDBx:auth_atom_id><PDBx:auth_comp_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">ALA</PDBx:auth_comp_id><PDBx:auth_seq_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">4</PDBx:auth_seq_id><PDBx:group_PDB xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">ATOM</PDBx:group_PDB><PDBx:label_alt_id xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:nil=\"true\" xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\"/><PDBx:label_asym_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">A</PDBx:label_asym_id><PDBx:label_atom_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">CA</PDBx:label_atom_id><PDBx:label_comp_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">ALA</PDBx:label_comp_id><PDBx:label_entity_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">1</PDBx:label_entity_id><PDBx:label_seq_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">4</PDBx:label_seq_id><PDBx:occupancy xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">1.00</PDBx:occupancy><PDBx:pdbx_PDB_model_num xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">1</PDBx:pdbx_PDB_model_num><PDBx:type_symbol xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">C</PDBx:type_symbol></string>"}


-- assigning atom_side id attribute as the key for each record entry, instead of "atom_site"

CREATE EXTERNAL TABLE ids_maps2(pdbid STRING, record array<map<string,string>> )
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName",
"column.xpath.record"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*",
"xml.map.specification.atom_site"="@id->#content"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/hive_all"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);

-- query returns: 
-- 11AS	{"1":"<string><PDBx:B_iso_or_equiv xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">35.74</PDBx:B_iso_or_equiv><PDBx:Cartn_x xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">11.746</PDBx:Cartn_x><PDBx:Cartn_y xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">37.328</PDBx:Cartn_y><PDBx:Cartn_z xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">28.300</PDBx:Cartn_z><PDBx:auth_asym_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">A</PDBx:auth_asym_id><PDBx:auth_atom_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">N</PDBx:auth_atom_id><PDBx:auth_comp_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">ALA</PDBx:auth_comp_id><PDBx:auth_seq_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">4</PDBx:auth_seq_id><PDBx:group_PDB xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">ATOM</PDBx:group_PDB><PDBx:label_alt_id xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:nil=\"true\" xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\"/><PDBx:label_asym_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">A</PDBx:label_asym_id><PDBx:label_atom_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">N</PDBx:label_atom_id><PDBx:label_comp_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">ALA</PDBx:label_comp_id><PDBx:label_entity_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">1</PDBx:label_entity_id><PDBx:label_seq_id xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">4</PDBx:label_seq_id><PDBx:occupancy xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">1.00</PDBx:occupancy><PDBx:pdbx_PDB_model_num xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">1</PDBx:pdbx_PDB_model_num><PDBx:type_symbol xmlns:PDBx=\"http://pdbml.pdb.org/schema/pdbx-v40.xsd\">N</PDBx:type_symbol></string>"}


-- try map of maps, maybe?





---- To avoid the problem of the fixed width columns, trying to load from the full file, but with columns..."

CREATE EXTERNAL TABLE fullxml(pdbid STRING, pdbx_PDB_model_num array<int>, label_asym_id array<string>, label_seq_id array<int>, label_comp_id array<string>,label_atom_id array<string>,cartn_x array<double>,cartn_y array<double>,cartn_z array<double>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName",
"column.xpath.pdbx_PDB_model_num"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']/*[local-name()='pdbx_PDB_model_num']/text()",
"column.xpath.label_asym_id"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']/*[local-name()='label_asym_id']/text()",
"column.xpath.label_seq_id"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']/*[local-name()='label_seq_id']/text()",
"column.xpath.label_comp_id"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']/*[local-name()='label_comp_id']/text()",
"column.xpath.label_atom_id"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']/*[local-name()='label_atom_id']/text()",
"column.xpath.cartn_x"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']/*[local-name()='Cartn_x']/text()",
"column.xpath.cartn_y"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']/*[local-name()='Cartn_y']/text()",
"column.xpath.cartn_z"="/*[local-name()='datablock']/*[local-name()='atom_siteCategory']/*[local-name()='atom_site']/*[local-name()='Cartn_z']/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/hive_all"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);

SELECT pdbid, size(pdbx_PDB_model_num),size(label_asym_id),size(label_seq_id),size(label_comp_id),size(label_atom_id),size(cartn_x),size(cartn_y),size(cartn_z)
from fullxml;
11AS	5220	5220	5118	5220	5220	5220	5220	5220
11BA	2126	2126	1925	2126	2126	2126	2126	2126

--this method is a problem because HETATM lines don't have a seq_id and don't know how to avoid those :S
<PDBx:group_PDB>HETATM</PDBx:group_PDB>
<PDBx:label_alt_id xsi:nil="true" />
<PDBx:label_asym_id>B</PDBx:label_asym_id>
<PDBx:label_atom_id>O</PDBx:label_atom_id>
<PDBx:label_comp_id>HOH</PDBx:label_comp_id>
<PDBx:label_entity_id>2</PDBx:label_entity_id>
<PDBx:label_seq_id xsi:nil="true" />


--this query to explode into rows takes ages on just two files!:S

SELECT pdbid, pdbx_PDB_model_num1,label_asym_id1,label_seq_id1,label_comp_id1,label_atom_id1,cartn_x1,cartn_y1,cartn_z1
FROM fullxml 
LATERAL VIEW explode(pdbx_PDB_model_num) t1 AS pdbx_PDB_model_num1
LATERAL VIEW explode(label_asym_id) t2 AS label_asym_id1
LATERAL VIEW explode(label_seq_id) t3 AS label_seq_id1
LATERAL VIEW explode(label_comp_id) t4 AS label_comp_id1
LATERAL VIEW explode(label_atom_id) t5 AS label_atom_id1
LATERAL VIEW explode(cartn_x) t6 AS cartn_x1
LATERAL VIEW explode(cartn_y) t7 AS cartn_y1
LATERAL VIEW explode(cartn_z) t8 AS cartn_z1;






