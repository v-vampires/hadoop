package hadoop07;

public class Parser
{
    public static String[] parse(String str, String templet, String separator)
    {
        String[] lines = templet.split(separator);
        int start = 0;
        String[] ret = new String[lines.length];
        for(int i = 0; i < lines.length; i++)
        {
            ret[i] = str.substring(start, start + lines[i].length());
            start += lines[i].length() + separator.length();
        }
        return ret;
    }
    
}
