<?php
/**
 * @author Daryl Blake
 * 
 *
 * Instructions
 * run like this:
 * $ php csvtotable.php clean filename.csv (to clean it)
 * $ php csvtotable.php clean_filename.csv (to run the cleaned file) - you can replace clean_filename.csv to a already clean csv.
 */


class CsvToDatabase
{
    private static $_server;
    private static $_user;
    private static $_password;
    private static $_database;
    private static $_databaseType;
    private static $_querySplitSize = 1;
    
    private $_filename;
    private $_headerMap = array();
    private $_data = array();
    private $_columnAttributes = array(); //will either be INT/DECIMAL OR the number of characters needed.
    private $_columnCount = 0;
    private $_rowCount = 0;
    private $_pdoConnection;
    private $_tableName;
    private $_fileNameOnly = "";
    
    public function __construct($filename) 
    {
        $this->_init();
        $this->_filename = $filename;
        if(File::isFullPath($filename))
        {
            $this->_fileNameOnly = File::getFileNameFromFullPath($filename);
        } 
    }
    
    private function _init()
    {
        $connector = json_decode(file_get_contents("config.json"));
        static::$_server = $connector["dbparams"]["dbserver"];
        static::$_user = $connector["dbparams"]["user"];
        static::$_password = $connector["dbparams"]["password"];
        static::$_database = $connector["dbname"];
        static::$_databaseType = $connector["dbtype"];
    }
    
    private function _extractTheShit()
    {
        $fp = fopen($this->_filename,'r');
        if( $fp === FALSE)
        {
            echo "\n Cannot open file {$this->_filename}";
            exit(0);
        }
        $line = 0;
        while($recordLine = fgetcsv($fp))
        {
            if($line==0)
            {
                $this->_headerMap = $recordLine; 
            } 
            else
            {                
                $this->_data[] = $recordLine; 
            }
            $line++;
        }
    }
    
    public function go()
    {  
        $this->_extractTheShit();
        $this->_findOutWhatTheHellWeNeedToMake();
        $this->_connectToDb();
        $this->_createTheTable();
        $this->_importTheData();
        $this->_disconnectFromDb();
    }
    
    private function _connectToDb()
    {      
        $this->_pdoConnection = new PDO( self::$_databaseType.':dbname='.self::$_database.';host='.self::$_server, self::$_user, self::$_password );
    }
    
    private function _disconnectFromDb()
    {
        unset($this->_pdoConnection);
    }
    
    private static function _placeholders($text, $count=0, $separator=","){
        $result = array();
        if($count > 0){
            for($x=0; $x<$count; $x++){
                $result[] = $text;
            }
        }
        return implode($separator, $result);
    }
    
    private function _importTheData()
    {
        $this->_pdoConnection->beginTransaction();
        $insert_values = array();
        $question_marks = array();
        $i = 0;
        foreach($this->_data as $dataArray)
        {
            if($i>self::$_querySplitSize)
            {
                $sql = "INSERT INTO `{$this->_tableName}` (`".implode("`,`", array_values($this->_headerMap))."`) VALUES ".implode(',', $question_marks );
                $stmt = $this->_pdoConnection->prepare ($sql);
                echo "\nExecuting Import of ".self::$_querySplitSize." rows.  ";
                $time = microtime();
                $stmt->execute($insert_values);
                $endtime = microtime();
                echo " Completed in ".($endtime-$time)." milliseconds";
                $insert_values = array();
                $question_marks = array();
                $i=0;
            }
            $question_marks[] = '('.self::placeholders('?', count($dataArray)).')';
            $insert_values = array_merge($insert_values, array_values($dataArray));
            $i++;
        }
        $sql = "INSERT INTO `{$this->_tableName}` (`".implode("`,`", array_values($this->_headerMap))."`) VALUES ".implode(',', $question_marks );
        $stmt = $this->_pdoConnection->prepare ($sql);
        echo "\nExecuting Import of whatever rows are left. ";
        $time = microtime();
        $stmt->execute($insert_values);
        $endtime = microtime();
        echo "Completed in ".$endtime-$time." milliseconds";
        $this->_pdoConnection->commit();
    }
    
    private function _importTheDataSingleQuery() //Does a single insert. 
    {
        $this->_pdoConnection->beginTransaction();
        $insert_values = array();
        $question_marks = array();
        foreach($this->_data as $dataArray)
        {
            $question_marks[] = '('.self::placeholders('?', count($dataArray)).')';
            $insert_values = array_merge($insert_values, array_values($dataArray));
        }
        $sql = "INSERT INTO `{$this->_tableName}` (`".implode("`,`", array_values($this->_headerMap))."`) VALUES ".implode(',', $question_marks );
        $stmt = $this->_pdoConnection->prepare ($sql);
        $time = microtime();
        $stmt->execute($insert_values);
        $endtime = microtime();
        echo "Completed in ".$endtime-$time." milliseconds";
        $this->_pdoConnection->commit();
    }
    
    private function _createTheTable()
    {   
        $this->_pdoConnection->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
        $this->_tableName = str_replace(".csv", "", $this->_fileNameOnly?$this->_fileNameOnly:$this->_filename);
        $sql = $this->_generateCreateStatement($this->_tableName);
        $this->_pdoConnection->exec($sql);
    }
    
    private function _generateCreateStatement($tableName)
    {
        return "CREATE TABLE `{$tableName}`( "
        . "`id` INT(11) AUTO_INCREMENT PRIMARY KEY ".$this->_createColumnSql()." );";
    }
    
    private function _createColumnSql()
    {
        $line = "";
        foreach($this->_headerMap as $ordinalPosition => $headerColumnName)
        {
            $line.=", `$headerColumnName` ".self::_processColumnAttribute($this->_columnAttributes[$ordinalPosition])." NULL DEFAULT NULL";
        }
        return $line;
    }
    
    private static function _processColumnAttribute($columnAttribute)
    {
        switch($columnAttribute)
        {
            case "DECIMAL": return "DECIMAL(16,4)";
            case "INT": return "INT(11)";
            default:
                if($columnAttribute<245)
                {
                    return "VARCHAR(".($columnAttribute+10).")";
                } 
                else
                {
                    return "MEDIUMTEXT";
                }
        }
    }

    private function _findOutWhatTheHellWeNeedToMake()
    {
        $this->_columnCount = count($this->_headerMap);
        $this->_rowCount = count($this->_data);
        for($i=0;$i<$this->_columnCount; $i++)
        {
            $this->_processColumn($i);
        }
    }
    
    private function _processColumn($ordinalPosition)
    {
        for($i=0;$i<$this->_columnCount; $i++)
        {
            $this->_evaluate( $this->_data[$i][$ordinalPosition], $ordinalPosition );
        }
    }
    
    private function _evaluate($str, $ordinalPosition)//was a bit of a nightmare. to get this getting the right values.
    {
        
        if(isset($this->_columnAttributes[$ordinalPosition]) 
                && self::isNumeric($this->_columnAttributes[$ordinalPosition]) 
                && strlen($str) > $this->_columnAttributes[$ordinalPosition])
        {
            $this->_columnAttributes[$ordinalPosition] = strlen($str);
        } 
        else 
        {
            if(self::isAFloat($str))
            {
                $this->_columnAttributes[$ordinalPosition] = "DECIMAL";
            } 
            else if(self::isNumeric($str))
            {
                if(!$this->_columnAttributes[$ordinalPosition] == "DECIMAL")
                {
                    $this->_columnAttributes[$ordinalPosition] = "INT";
                }
            } 
            else
            {
                $length = strlen($str);
                if(isset($this->_columnAttributes[$ordinalPosition]) && $this->_columnAttributes[$ordinalPosition] < $length )
                {
                    $this->_columnAttributes[$ordinalPosition] = $length;
                } 
                if(!isset($this->_columnAttributes[$ordinalPosition]))
                {
                    $this->_columnAttributes[$ordinalPosition] = $length;
                }
            }
        }
    }
    
    public static function isNumeric($val)
    {
        if(intval($val) === 0 && ($val != "0"))
        {
            return false;
        } 
        else
        {
            if(is_numeric($val))
            {
                return true;
            } 
            else
            {
                return false;
            }
        }
    }
    
    public static function placeholders($text, $count=0, $separator=",")
    {
        $result = array();
        if($count > 0){
            for($x=0; $x<$count; $x++){
                $result[] = $text;
            }
        }
        return implode($separator, $result);
    }
    
    public static function isAFloat($str)
    {
        if(strpos($str, ".") && !preg_match('/[A-z]+/', $str))
        {
            $floatval = floatval($str);
            if( ($floatval === 1 || $floatval === 0))
            {
                return false;
            } else {
                return true;
            }
        }
        return false;
    }
    
    public static function performClean($file)
    {
        $fp = fopen($file,'r');
        if(File::isFullPath($file))
        {
            $fileName = File::getFileNameFromFullPath($file);
            $pathComponent = File::getPathComponentFromFullPath($file);
            $newPath = $pathComponent."clean_".$fileName;
            echo "New Path ".$newPath."\n";
        } 
        else
        {
            $newPath = "clean_".$file;
        }
        $writeFilePointer = fopen($newPath, 'w');
        if($writeFilePointer===FALSE) { die("Cannot open output file pointer ".$newPath); } 
        while(($line = fgets($fp)) !== false)
        {
            $newline = str_replace("\\\\\"", "\"", $line);
            $newline = str_replace("\\\"", "\"", $newline);
            fwrite($writeFilePointer, $newline);
        }
        fclose($fp);
        fclose($writeFilePointer);
    }
}

class File 
{
    public static function isFullPath($file)
    {
        return is_int(strpos($file, "/"))?true:false;
    }
    
    public static function getFileNameFromFullPath($pathToFile)
    {
        $array = explode("/",$pathToFile);
        return $array[count($array)-1]?$array[count($array)-1]:$pathToFile;
    }
    
    public static function getPathComponentFromFullPath($pathToFile)
    {
        $array = explode("/",$pathToFile);
        unset($array[count($array)-1]);;
        return implode("/", $array)."/";
    }
}

if(!isset($argv[1]))
{
    echo "\n File not specified";
    exit(0);
}
if($argv[1] == "clean")
{
    CsvToDatabase::performClean($argv[2]);
    echo "File cleaned ".$argv[2]."\n";
    exit(0);
}

$csvToDB = new CsvToDatabase($argv[1]);
try 
{
    $csvToDB->go();
} 
catch (PDOException $e) 
{
    echo 'Connection failed: ' . $e->getMessage();
} catch (Exception $ex) {
    echo 'Connection failed: ' . $e->getMessage();
}


 
?>
