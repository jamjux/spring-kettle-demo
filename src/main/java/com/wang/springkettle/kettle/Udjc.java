package com.wang.springkettle.kettle;

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.userdefinedjavaclass.TransformClassBase;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClass;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassData;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassMeta;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Udjc extends TransformClassBase {

    private String[] masFields = new String[]{"COMM_TYPE", "COMM_METHOD", "COMM_FARE", "COMM_CALCULATE", "COMM_MIN", "MAS_FILE_ID", "PROTL_DATE_FROM", "PROTL_DATE_TO"};
    private Map<String, Object> fieldsMap = new HashMap<>();
    private List<Map<String, Object>> rowList = new ArrayList<>();

    public Udjc(UserDefinedJavaClass parent, UserDefinedJavaClassMeta meta, UserDefinedJavaClassData data) throws KettleStepException {
        super(parent, meta, data);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        if (first) {
            first = false;

            RowSet infoStream = findInfoRowSet("MAS_PAYPROTOCOL");

            Object[] infoRow = null;

            // Read all rows from info step before calling getRow() method, which returns first row from any
            // input rowset. As rowMeta for info and input steps varies getRow() can lead to errors.
            while ((infoRow = getRowFrom(infoStream)) != null) {

                for (int i = 0; i < masFields.length; i++) {
                    if ("PROTL_DATE_FROM".equals(masFields[i])) {
                        fieldsMap.put(masFields[i], this.get(Fields.Info, masFields[i]).getDate(infoRow));
                    } else {
                        fieldsMap.put(masFields[i], this.get(Fields.Info, masFields[i]).getString(infoRow));
                    }
                }
                rowList.add(fieldsMap);

            }

        }

        Object[] r = getRow();

        if (r == null) {
            setOutputDone();
            return false;
        }

        // It is always safest to call createOutputRow() to ensure that your output row's Object[] is large
        // enough to handle any new fields you are creating in this step.
        r = createOutputRow(r, data.outputRowMeta.size());


        // Get the value from an input field
        Date transDate = get(Fields.In, "TRANS_DATE").getDate(r);

        for (Map<String, Object> row: rowList) {
            Date protlDateFrom = (Date)row.get("PROTL_DATE_FROM");
            Date protlDateTo = (Date)row.get("PROTL_DATE_TO");

            if(transDate.getTime() >= protlDateFrom.getTime() && transDate.getTime() <= protlDateTo.getTime()){

                Set<Map.Entry<String, Object>> entries = row.entrySet();
                for (Map.Entry<String, Object> entry : entries) {

                    // Set a value in a new output field
                    get(Fields.Out, entry.getKey()).setValue(r, entry.getValue());
                }
            }



        }

        // Send the row on to the next step.
        putRow(data.outputRowMeta, r);

        return true;
    }
}