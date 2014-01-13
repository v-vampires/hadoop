package hadoop07;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DeptWritable implements Writable
{
    private Text deptNo;
    
    private Text dName;
    
    private Text loc;
    
    public DeptWritable()
    {
    }
    
    public DeptWritable(String deptNo, String dName, String loc)
    {
        this.deptNo = new Text(deptNo);
        this.dName = new Text(dName);
        this.loc = new Text(loc);
    }
    
    public Text getDeptNo()
    {
        return deptNo;
    }
    
    public void setDeptNo(Text deptNo)
    {
        this.deptNo = deptNo;
    }
    
    public Text getdName()
    {
        return dName;
    }
    
    public void setdName(Text dName)
    {
        this.dName = dName;
    }
    
    public Text getLoc()
    {
        return loc;
    }
    
    public void setLoc(Text loc)
    {
        this.loc = loc;
    }
    
    @Override
    public void write(DataOutput out) throws IOException
    {
        
        deptNo.write(out);
        dName.write(out);
        loc.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException
    {
        deptNo.readFields(in);
        dName.readFields(in);
        loc.readFields(in);
    }
    
}
