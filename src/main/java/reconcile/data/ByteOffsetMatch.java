package reconcile.data;

public class ByteOffsetMatch {

public String value;

public int start;

public int end;

public ByteOffsetMatch() {
}

public ByteOffsetMatch(String v, int start, int end) {
  value = v;
  this.start = start;
  this.end = end;
}

@Override
public String toString()
{
  return value + "(" + start + "," + end + ")";
}

public String manualJSON()
{
  return "{\"value\":\"" + value + "\",\"start\":" + start + ",\"end\":" + end + "}";

}
}