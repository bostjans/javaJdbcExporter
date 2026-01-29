package com.stupica.exporter;


import com.stupica.ConstGlobal;
import com.stupica.GlobalVar;
import com.stupica.core.UtilString;
import com.stupica.jdbc.ConnectionHandler;
import com.stupica.jdbc.StatementHandler;

import jargs.gnu.CmdLineParser;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by bostjans on 07/09/16.
 */
public class MainRun {
    // Variables
    //
    boolean bIsModeTest = true;
    boolean bIsModeVerbose = true;

    boolean bIsHeaderInFile = true;

    boolean bIsEndOfData = false;

    boolean bIsOutputSql = true;
    boolean bIsOutputHeader = true;

    //int     iNumOfFieldMax = 99;

    long    iMaxRows = 13;

    final int     iMaxFieldSize = 256;

    String  sDelimiter = ";";
    String  sDtFormat = "yyyy-MM-dd HH:mm:ss";

    String  sFileInput = null;
    String  sJdbcConn = "jdbc:mysql://localhost/test";

    String  sJdbcUser = "lenkoApp";
    String  sJdbcPsw = "lenkoApp";

    //String  sTable = "test";
    //String  sOperation = "SELECT";
    //String  sField = "";
    int[]       arrFieldLength = null;

    FileReader      objReader = null;
    BufferedReader  obReaderBuff = null;

    /**
     * Main object instance variable;
     */
    private static MainRun objInstance;

    private static Logger logger = Logger.getLogger(MainRun.class.getName());

    private SimpleDateFormat objDateFormat01 = new SimpleDateFormat(sDtFormat);

    private ConnectionHandler objConnHandler = null;
    private StatementHandler objStatHandler = null;


    /**
     * @param a_args    ..
     */
    public static void main(String[] a_args) {
        // Local variables
        int             i_result;
        int             i_return;

        // Initialization
        i_result = ConstGlobal.RETURN_OK;
        //
        i_return = ConstGlobal.PROCESS_EXIT_SUCCESS;
        GlobalVar.getInstance().sProgName = "exporter.csv";
        GlobalVar.getInstance().sVersionMax = "0";
        GlobalVar.getInstance().sVersionMin = "1";
        GlobalVar.getInstance().sVersionPatch = "0";
        GlobalVar.getInstance().sVersionBuild = "17";
        GlobalVar.getInstance().sAuthor = "stupica.com - Bostjan Stupica";

        // Generate main program class
        objInstance = new MainRun();

        if (objInstance.bIsModeTest) {
            if (logger != null) {
                //logger.setLevel(java.util.logging.Level.ALL);
                logger.setLevel(Level.FINE);

                ConsoleHandler handler = new ConsoleHandler();
                // PUBLISH this level
                handler.setLevel(Level.FINE);
                logger.addHandler(handler);
                //
                logger.setUseParentHandlers(false);
            }
        }

        // Program parameters
        //
        // Create a CmdLineParser, and add to it the appropriate Options.
        CmdLineParser obj_parser = new CmdLineParser();
        CmdLineParser.Option obj_op_help = obj_parser.addBooleanOption('h', "help");
        CmdLineParser.Option obj_op_quiet = obj_parser.addBooleanOption('q', "quiet");
        CmdLineParser.Option obj_op_source = obj_parser.addStringOption('s', "source");
        CmdLineParser.Option obj_op_dest = obj_parser.addStringOption('d', "dest");
        CmdLineParser.Option obj_op_user = obj_parser.addStringOption('u', "user");
        CmdLineParser.Option obj_op_psw = obj_parser.addStringOption('p', "psw");
        CmdLineParser.Option obj_op_maxRows = obj_parser.addLongOption('m', "maxrows");
        //CmdLineParser.Option obj_op_oper = obj_parser.addStringOption('o', "operation");
        CmdLineParser.Option obj_op_csv = obj_parser.addStringOption('c', "csv");
        //CmdLineParser.Option obj_op_field = obj_parser.addStringOption('f', "field");
        //CmdLineParser.Option obj_op_fieldCheck = obj_parser.addStringOption("fieldCheck");

        try {
            obj_parser.parse(a_args);
        } catch (CmdLineParser.OptionException e) {
            System.err.println(e.getMessage());
            print_usage();
            System.exit(ConstGlobal.PROCESS_EXIT_FAIL_PARAM);
        }

        if (Boolean.TRUE.equals(obj_parser.getOptionValue(obj_op_help))) {
            print_usage();
            System.exit(ConstGlobal.PROCESS_EXIT_SUCCESS);
        }
        if (!Boolean.TRUE.equals(obj_parser.getOptionValue(obj_op_quiet)))
        {
            // Display program info
            System.out.println();
            System.out.println("Program: " + GlobalVar.getInstance().sProgName);
            System.out.println("Version: " + GlobalVar.getInstance().get_version());
            System.out.println("Made by: " + GlobalVar.getInstance().sAuthor);
            System.out.println("===");
            // Check logger
            if (logger != null) {
                logger.info("main(): Program is starting ..");
            }
        } else {
            objInstance.bIsModeVerbose = false;
        }

        // Check previous step
        if (i_return == ConstGlobal.PROCESS_EXIT_SUCCESS) {
            // Set program parameter
            objInstance.sFileInput = (String)obj_parser.getOptionValue(obj_op_source, "");
            if (objInstance.sFileInput.isEmpty()) {
                objInstance.sFileInput = "testData" + File.separator + "fileIn.sql";
                if (objInstance.bIsModeTest) {
                    objInstance.sFileInput = "testData" + File.separator + "fileIn01.sql";
                }
            }
        }
        // Check previous step
        if (i_return == ConstGlobal.PROCESS_EXIT_SUCCESS) {
            // Set program parameter
            objInstance.sJdbcConn = (String)obj_parser.getOptionValue(obj_op_dest, objInstance.sJdbcConn);
            objInstance.sJdbcUser = (String)obj_parser.getOptionValue(obj_op_user, "test");
            objInstance.sJdbcPsw = (String)obj_parser.getOptionValue(obj_op_psw, "");
        }
        // Check previous step
        if (i_return == ConstGlobal.PROCESS_EXIT_SUCCESS) {
            Integer iTemp;
            // Set program parameter
            iTemp = (Integer)obj_parser.getOptionValue(obj_op_maxRows, -1);
            objInstance.iMaxRows = iTemp.longValue();
            //objInstance.arrField[0] = objInstance.sTable;
        }
        // Check previous step
        if (i_return == ConstGlobal.PROCESS_EXIT_SUCCESS) {
            // Set program parameter
            objInstance.sDelimiter = (String)obj_parser.getOptionValue(obj_op_csv, objInstance.sDelimiter);
        }

        // Check previous step
        if (i_return == ConstGlobal.PROCESS_EXIT_SUCCESS) {
            // Run ..
            i_result = objInstance.run();
            // Error
            if (i_result != ConstGlobal.RETURN_OK) {
                logger.severe("main(): Error at run() operation!");
                i_return = ConstGlobal.PROCESS_EXIT_FAILURE;
            }
        }

        // Return
        if (i_return != ConstGlobal.PROCESS_EXIT_SUCCESS)
            System.exit(i_return);
        else
            //System.exit(ConstGlobal.PROCESS_EXIT_SUCCESS);
            return;
    }


    private static void print_usage() {
        System.err.println("Usage: prog [-h,--help]");
        System.err.println("            [-q,--quiet]");
        System.err.println("            [{-s,--source} a_file_input] .. where SQL (Select) statements are;");
        System.err.println("            [{-d,--dest} a_jdbc_url]");
        System.err.println("            [{-u,--user} a_jdbc_username]");
        System.err.println("            [{-p,--psw} a_jdbc_password]");
        System.err.println("            [{-m,--maxrows} Max_number_of_rows]");
        System.err.println("            [{-c,--csv} CSV_delimiter]");
        //System.err.println("            [{-f,--field(s)} field(s) definition");
    }


    /**
     * Method: run
     *
     * Run ..
     *
     * @return int	1 = AllOK;
     */
    public int run() {
        // Local variables
        int         iResult;
        File        objFileIn = null;

        // Initialization
        iResult = ConstGlobal.RETURN_SUCCESS;

        // Open IN file ..
        //
        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            if (sFileInput == null) {
                iResult = ConstGlobal.RETURN_ERROR;
                logger.severe("run(): Error at input file verification - input file NOT defined!"
                        + " FileIN: " + sFileInput);
            }
        }
        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            objFileIn = new File(sFileInput);
            if (objFileIn == null) {
                iResult = ConstGlobal.RETURN_ERROR;
                logger.severe("run(): Error at input file verification!"
                        + " FileIN: " + sFileInput);
            }
        }
        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            if (!objFileIn.exists()) {
                iResult = ConstGlobal.RETURN_ERROR;
                logger.severe("run(): Error at input file verification - does NOT exists!"
                        + " FileIN: " + sFileInput
                        + " FileIN(Abs.): " + objFileIn.getAbsolutePath());
            }
        }
        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            if (!objFileIn.canRead()) {
                iResult = ConstGlobal.RETURN_ERROR;
                logger.severe("run(): Error at input file verification - can NOT read!"
                        + " FileIN: " + objFileIn.getAbsolutePath());
            }
        }

        // Prepare object ..
        //
        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            try {
                objReader = new FileReader(objFileIn);
                obReaderBuff = new BufferedReader(objReader);
            } catch(FileNotFoundException ex) {
                iResult = ConstGlobal.RETURN_ERROR;
                logger.severe("run(): Error at openFile() operation!");
            }
        }

        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            objConnHandler = new ConnectionHandler();
            objConnHandler.setDbParam(sJdbcConn, null, sJdbcUser, sJdbcPsw);
            objStatHandler = new StatementHandler();
            objStatHandler.setConnection(objConnHandler);

            iResult = objConnHandler.connect2db();
            // Error
            if (iResult != ConstGlobal.RETURN_OK) {
                logger.severe("run(): Error at connect2db() operation!");
            }
        }

        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            // Run ..
            iResult = process();
            // Error
            if (iResult != ConstGlobal.RETURN_OK) {
                logger.severe("run(): Error at process() operation!");
            }
        }
        // Return
        return iResult;
    }


    /**
     * Method: process
     *
     * Run ..
     *
     * @return int	1 = AllOK;
     */
    public int process() {
        // Local variables
        int         iResult;
        StringBuilder sStatement = null;
        //
        long        iCountData = 0L;
        long        iCountDataSkip = 0L;
        Date        dtStart = null;
        Date        dtStop = null;
        //
        List<Map<String,Object>> arrMapData = null;

        // Initialization
        iResult = ConstGlobal.RETURN_SUCCESS;
        sStatement = new StringBuilder();
        //arrFieldLength = new int[iNumOfFieldMax];
        dtStart = new Date();

        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            do {
                sStatement.delete(0, sStatement.length());

                iResult = readStatementSql(sStatement);
                // Error
                if (iResult != ConstGlobal.RETURN_OK) {
                    logger.severe("process(): Error at readStatementSql() operation!");
                    break;
                }
                if (sStatement.length() < 1) {
                    if (bIsEndOfData) {
                        break;
                    }
                    continue;
                }
                iCountData++;

                // Execute SQL ..
                if (iMaxRows > 0) {
                    arrMapData = objStatHandler.readMapListCount(sStatement.toString(), iMaxRows, null);
                } else {
                    arrMapData = objStatHandler.readMapList(sStatement.toString());
                }
                if (arrMapData == null) {
                    //iResult = ConstGlobal.RETURN_NODATA;
                    logger.warning("process(): No data retrieved (NULL)!"
                            + " Msg.: /"
                            + "; SQL: " + sStatement.toString());
                } else {
                    if (bIsOutputSql) {
                        System.out.println();
                        System.out.println("> " + sStatement.toString());
                    }
                    arrFieldLength = objStatHandler.getArrFieldLength();

                    iResult = processResultSql(arrMapData);
                    // Error
                    if (iResult != ConstGlobal.RETURN_OK) {
                        logger.severe("process(): Error at processResultSql() operation!");
                        break;
                    }
                }

                if (bIsEndOfData) {
                    break;
                }
            } while (!bIsEndOfData);
        }

        dtStop = new Date();
        logger.info("process(): Processing done."
                + "\n\tbIsHeaderInFile: " + bIsHeaderInFile
                + "\n\tData num.: " + iCountData
                + "\n\tData skiped: " + iCountDataSkip
                + "\n\tDuration(ms): " + (dtStop.getTime() - dtStart.getTime()));
        // Return
        return iResult;
    }


    /**
     * Method: readStatementSql
     *
     * Run ..
     *
     * @return int	1 = AllOK;
     */
    private int readStatementSql(StringBuilder asStatement) {
        // Local variables
        int         iResult;
        int         iCountEmptyLine;
        int         iEmptyLinesAllowed = 2;
        String      sLine = null;

        // Initialization
        iResult = ConstGlobal.RETURN_SUCCESS;
        iCountEmptyLine = 0;

        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            do {
                try {
                    sLine = obReaderBuff.readLine();
                    logger.info("readStatementSql(): Line: " + sLine);
                } catch(IOException ex) {
                    iResult = ConstGlobal.RETURN_ERROR;
                    logger.severe("readStatementSql(): Error at read() operation!"
                            + " Msg.: " + ex.getMessage());
                }
                if (sLine == null) {
                    bIsEndOfData = true;
                    break;
                }
                if (sLine.trim().startsWith("--")) {    // This is just a comment .. continue.
                    continue;
                }
                if (UtilString.isEmptyTrim(sLine)) {
                    iCountEmptyLine++;
                    if (iCountEmptyLine > (iEmptyLinesAllowed - 1)) {
                        break;
                    }
                    continue;
                } else {
                    iCountEmptyLine = 0;
                }
                asStatement.append(sLine);
                if (asStatement.indexOf(";") > -1) {
                    break;
                }
            } while (sLine != null);
        }

        if (bIsEndOfData) {
            if (obReaderBuff != null) {
                try {
                    obReaderBuff.close();
                } catch(IOException ex) {
                    iResult = ConstGlobal.RETURN_ERROR;
                    logger.severe("readStatementSql(): Error at close() operation!"
                            + " Msg.: " + ex.getMessage());
                }
            }
        }
        return iResult;
    }


    /**
     * Method: processResultSql
     *
     * Run ..
     *
     * @return int	1 = AllOK;
     */
    private int processResultSql(List<Map<String, Object>> aarrMapData) {
        // Local variables
        int             iResult;
        boolean         bIsFirstLine = true;
        int             iCountColumn = 0;
        //int             iCountColumnMax = 0;
        StringBuilder   sOutput = new StringBuilder();
        StringBuilder   sLine = new StringBuilder();

        // Initialization
        iResult = ConstGlobal.RETURN_SUCCESS;

        // Check
        if (aarrMapData.isEmpty()) {
            sOutput.append("No data.\n");
            iResult = ConstGlobal.RETURN_NODATA;
        }

        // Check previous step
        if (iResult == ConstGlobal.RETURN_OK) {
            for (Map<String, Object> objLoop : aarrMapData) {
                sLine.delete(0, sLine.length());

                if (bIsFirstLine) {
                    if (bIsOutputHeader) {
                        iCountColumn = 0;
                        for (String sKey : objLoop.keySet()) {
                            iCountColumn++;
                            if (iCountColumn > 1) {
                                sLine.append(sDelimiter).append(" ");
                            }
                            if (arrFieldLength != null) {
                                if (arrFieldLength[iCountColumn - 1] < sKey.length()) {
                                    arrFieldLength[iCountColumn - 1] = sKey.length();
                                }
                            }
                            String sTemp = outputStringAlign(sKey, (iCountColumn - 1), -1);
                            sLine.append(sTemp);
                            //System.out.println("\tKey: " + sKey);
                        }
                        //iCountColumnMax = iCountColumn;
                        sLine.append("\n");
                        sOutput.append(sLine);
                        sLine.delete(0, sLine.length());
                    }
                    bIsFirstLine = false;
                }

                iCountColumn = 0;
                for (String sKey : objLoop.keySet()) {
                    Object objTemp = objLoop.get(sKey);

                    iCountColumn++;
                    if (iCountColumn > 1) {
                        sLine.append(sDelimiter).append(" ");
                    }
                    String sTemp = outputStringForField(objTemp, iCountColumn);
                    // Error
                    if (sTemp.startsWith("outputStringForField():")) {
                        iResult = ConstGlobal.RETURN_ERROR;
                        logger.severe("processResultSql(): Error in method: outputStringForField()"
                                + " Column count: " + iCountColumn);
                        break;
                    }
                    sLine.append(sTemp);
                }
                if (iResult != ConstGlobal.RETURN_OK) {
                    break;
                }
                sLine.append("\n");
                sOutput.append(sLine);
            }
        }

        // Check previous step
        if (iResult == ConstGlobal.RETURN_NODATA) {
            iResult = ConstGlobal.RETURN_OK;
        }
        if (iResult == ConstGlobal.RETURN_OK) {
            System.out.print(sOutput.toString());
        }
        return iResult;
    }


    /**
     * Method: outputStringForField
     *
     * Run ..
     *
     * @return String	1 = AllOK;
     */
    private String outputStringForField(Object aobjField, int aiColumnIndex) {
        int     iCountColumn = aiColumnIndex - 1;
        String  sOutput = null;

        if (aobjField == null) {
            sOutput = outputStringAlign("null", iCountColumn, -1);
            return sOutput;
        }

        if (aobjField instanceof String) {
            //String sTemp = (String)aobjField;
            sOutput = outputStringAlign((String)aobjField, iCountColumn, -1);
        } else if (aobjField instanceof Integer) {
            Integer iTemp = (Integer)aobjField;
            sOutput = outputStringAlign(iTemp.toString(), iCountColumn, 1);
        } else if (aobjField instanceof Long) {
            Long iTemp = (Long)aobjField;
            sOutput = outputStringAlign(iTemp.toString(), iCountColumn, 1);
        } else if (aobjField instanceof Short) {
            Short iTemp = (Short)aobjField;
            sOutput = outputStringAlign(iTemp.toString(), iCountColumn, 1);
        } else if (aobjField instanceof Date) {
            Date dtTemp = (Date)aobjField;
            sOutput = outputStringAlign(objDateFormat01.format(dtTemp), iCountColumn, 1);
        } else if (aobjField instanceof Double) {
            Double nTemp = (Double)aobjField;
            sOutput = outputStringAlign(nTemp.toString(), iCountColumn, 1);
        } else {
            sOutput = "outputStringForField(): Unknown result field type!"
                    + " Column count: " + iCountColumn
                    + " object_type: " + aobjField.toString();
            logger.severe(sOutput);
        }
        return sOutput;
    }

    /**
     * Method: outputStringForField
     *
     * Run ..
     *
     * @return String	1 = AllOK;
     */
    private String outputStringAlign(String asVal, int aiColumnIndex, int aiAlign) {
        String  sOutput = asVal;
        int     iMaxFieldSizeLocal = iMaxFieldSize;

        if (arrFieldLength != null) {
            if (asVal.length() > arrFieldLength[aiColumnIndex]) {
                arrFieldLength[aiColumnIndex] = asVal.length();
            }

            if (arrFieldLength[aiColumnIndex] < iMaxFieldSizeLocal) {
                iMaxFieldSizeLocal = arrFieldLength[aiColumnIndex];
            }

            if (aiAlign == 1) {         // Align: right
                //while (arrFieldLength[aiColumnIndex] > sOutput.length()) {
                while (iMaxFieldSizeLocal > sOutput.length()) {
                    sOutput = " " + sOutput;
                }
            } else if (aiAlign == 0) {  // Align: center

            } else {                    // Align: left
                //while (arrFieldLength[aiColumnIndex] > sOutput.length()) {
                while (iMaxFieldSizeLocal > sOutput.length()) {
                    sOutput = sOutput + " ";
                }
            }
        }
        return sOutput;
    }
}
