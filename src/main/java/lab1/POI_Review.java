package lab1;

public class POI_Review {
	String review_id;
	double longitude;
	double latitude;
	double height;
	String review_data;
	String temperature;
	double rating;
	String user_id;
	String user_birthday;
	String user_nationality;
	String user_career;
	double user_income;
	
	public POI_Review(String review_id, double longitude, double latitude, double height, String review_data,
			String temperature, double rating, String user_id, String user_birthday,
			String user_nationality, String user_career, double user_income){
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
}
