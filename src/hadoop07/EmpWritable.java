package hadoop07;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EmpWritable implements Writable
{
    private Text empNo;
    
    private Text eName;
    
    private Text job;
    
    private Text mgr;
    
    private Text hiredate;
    
    private IntWritable sal;
    
    private IntWritable comm;
    
    private Text deptNo;
    
    public EmpWritable()
    {
        // TODO Auto-generated constructor stub
    }
    
    public EmpWritable(String empNo, String eName, String job, String mgr, String hiredate, int sal, int comm, String deptNo)
    {
        this.empNo = new Text(empNo);
        this.eName = new Text(eName);
        this.job = new Text(job);
        this.mgr = new Text(mgr);
        this.hiredate = new Text(hiredate);
        this.sal = new IntWritable(sal);
        this.comm = new IntWritable(comm);
        this.deptNo = new Text(deptNo);
    }
    
    @Override
    public void write(DataOutput out) throws IOException
    {
        empNo.write(out);
        eName.write(out);
        job.write(out);
        mgr.write(out);
        hiredate.write(out);
        sal.write(out);
        comm.write(out);
        deptNo.write(out);
        
    }
    
    @Override
    public void readFields(DataInput in) throws IOException
    {
        empNo.readFields(in);
        eName.readFields(in);
        job.readFields(in);
        mgr.readFields(in);
        hiredate.readFields(in);
        sal.readFields(in);
        comm.readFields(in);
        deptNo.readFields(in);
    }
    
    public Text getEmpNo()
    {
        return empNo;
    }
    
    public void setEmpNo(Text empNo)
    {
        this.empNo = empNo;
    }
    
    public Text geteName()
    {
        return eName;
    }
    
    public void seteName(Text eName)
    {
        this.eName = eName;
    }
    
    public Text getJob()
    {
        return job;
    }
    
    public void setJob(Text job)
    {
        this.job = job;
    }
    
    public Text getMgr()
    {
        return mgr;
    }
    
    public void setMgr(Text mgr)
    {
        this.mgr = mgr;
    }
    
    public Text getHiredate()
    {
        return hiredate;
    }
    
    public void setHiredate(Text hiredate)
    {
        this.hiredate = hiredate;
    }
    
    public IntWritable getSal()
    {
        return sal;
    }
    
    public void setSal(IntWritable sal)
    {
        this.sal = sal;
    }
    
    public IntWritable getComm()
    {
        return comm;
    }
    
    public void setComm(IntWritable comm)
    {
        this.comm = comm;
    }
    
    public Text getDeptNo()
    {
        return deptNo;
    }
    
    public void setDeptNo(Text deptNo)
    {
        this.deptNo = deptNo;
    }
    
}
