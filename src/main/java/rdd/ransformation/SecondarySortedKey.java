package rdd.ransformation;

import scala.Serializable;
import scala.math.Ordered;

/**
 * 自定义的二次排序key
 * Created by ligc on 2020/8/27 12:39
 */
public class SecondarySortedKey implements Ordered<SecondarySortedKey>, Serializable {
    private int first;
    private int second;

    public SecondarySortedKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    //为要进行排序的多个列，提供getter()和setter()方法

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int compare( SecondarySortedKey that) {
        if(this.first - that.first != 0)
            return this.first - that.first;
        return this.second - that.second;
    }

    @Override
    public boolean $less( SecondarySortedKey that) {
        if(this.first < that.first){
            return true;
        }else if(this.first == that.first && this.second < that.second){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater( SecondarySortedKey that) {
        if(this.first > that.first){
            return true;
        }else if(this.first == that.first && this.second > that.second){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq( SecondarySortedKey that) {
        if($less(that))
            return true;
        if(this.first == that.first &&this.second == that.second)
            return true;
        return false;
    }

    @Override
    public boolean $greater$eq( SecondarySortedKey that) {
        if($greater(that))
            return true;
        if(this.first == that.first && this.second == that.second)
            return  true;
        return false;
    }

    @Override
    public int compareTo( SecondarySortedKey that) {
        if(this.first - that.first != 0)
            return this.first - that.first;
        return this.second - that.second;
    }
}
