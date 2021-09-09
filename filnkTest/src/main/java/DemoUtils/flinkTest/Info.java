package DemoUtils.flinkTest;

public class Info {
    public int id;
    public String name;
    public String sex;
    public float score;

    public Info(){}   //要带有这个无参构造
    public Info(int id,String name,String sex,float score){
        this.id= id;
        this.name = name;
        this.sex = sex;
        this.score = score;
    }

    @Override
    public String toString() {
        return id+":"+name+":"+sex+":"+score;
    }
}
