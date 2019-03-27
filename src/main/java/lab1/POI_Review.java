package lab1;

import java.text.DecimalFormat;
import java.util.Random;

import scala.Serializable;

public class POI_Review implements Serializable, Comparable<POI_Review>{

	private static final long serialVersionUID = 1L;
	String review_id;
	String longitude;
	String latitude;
	String height;
	String review_data;
	String temperature;
	String rating;
	String user_id;
	String user_birthday;
	String user_nationality;
	String user_career;
	String user_income;
	
	public POI_Review(String review_id, String longitude, String latitude, String height, String review_data,
			String temperature, String rating, String user_id, String user_birthday,
			String user_nationality, String user_career, String user_income){
		this.review_id = review_id;
		this.longitude = longitude;
		this.latitude = latitude;
		this.height = height;
		this.review_data = review_data;
		this.temperature = temperature;
		this.rating = rating;
		this.user_id = user_id;
		this.user_birthday = user_birthday;
		this.user_nationality = user_nationality;
		this.user_career = user_career;
		this.user_income = user_income;
	}
	
	@Override
	public String toString(){
		return review_id + "|" + longitude + "|" + latitude + "|"
				 + height + "|" + review_data  + "|" + temperature + "|" + rating
				 + "|" + user_id + "|" + user_birthday + "|" + user_nationality + "|" + user_career + "|"
				 + user_income;
	}

	public int compareTo(POI_Review o) {
		if(Double.parseDouble(this.rating) - Double.parseDouble(o.rating)>0){
			return 1;
		} else if(Double.parseDouble(this.rating) - Double.parseDouble(o.rating)<0){
			return -1;
		} else{
			return 0;
		}
	}
	
}
