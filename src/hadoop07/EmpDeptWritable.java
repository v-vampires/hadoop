package hadoop07;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class EmpDeptWritable extends GenericWritable
{
    
    private static Class<? extends Writable>[] classes = new Class[]{DeptWritable.class, EmpWritable.class};
    
    public EmpDeptWritable()
    {
    }
    
    public EmpDeptWritable(Writable instance)
    {
        set(instance);
    }
    
    @Override
    protected Class<? extends Writable>[] getTypes()
    {
        return classes;
    }
    
}
