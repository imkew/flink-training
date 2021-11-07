package org.apche.flink.pojo;

public class BillFundEnd {
    public String f1;
    public int f2;
    public int f3;
    public int f4;
    public int f5;

    public BillFundEnd() {
    }

    public BillFundEnd(String f1, int f2, int f3, int f4, int f5) {
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.f5 = f5;
    }

    public String getF1() {
        return f1;
    }

    public void setF1(String f1) {
        this.f1 = f1;
    }

    public int getF2() {
        return f2;
    }

    public void setF2(int f2) {
        this.f2 = f2;
    }

    public int getF3() {
        return f3;
    }

    public void setF3(int f3) {
        this.f3 = f3;
    }

    public int getF4() {
        return f4;
    }

    public void setF4(int f4) {
        this.f4 = f4;
    }

    public int getF5() {
        return f5;
    }

    public void setF5(int f5) {
        this.f5 = f5;
    }

    @Override
    public String toString() {
        return "BillFundEnd{" +
                "f1='" + f1 + '\'' +
                ", f2=" + f2 +
                ", f3=" + f3 +
                ", f4=" + f4 +
                ", f5=" + f5 +
                '}';
    }
}
