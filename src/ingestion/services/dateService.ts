import { Injectable } from '@nestjs/common';
@Injectable()
export class DateService{
    
    getCurrentTime24(): string {
    const currentDate = new Date();
    const currentHour = currentDate.getHours();
    const currentMinute = currentDate.getMinutes();
    return `${currentHour}:${currentMinute}`;
  }
  
  // Function to check if the current time is greater than 12 PM
   isTimeGreaterThan12PM(timeString) {
    const [hours, minutes] = timeString.split(':');
    const hour = parseInt(hours, 10);
    const minute = parseInt(minutes, 10);
  
    // Check if it's past 12 PM (noon)
    return hour > 12 || (hour === 12 && minute > 0);
  }
  
   getCurrentTimeForCronSchedule(){
    let currentTime = this.getCurrentTime24()
      if (this.isTimeGreaterThan12PM(currentTime)) {
    // Convert to IST (add 5 hours and 30 minutes for the UTC+5:30 offset)
    const [hours, minutes] = currentTime.split(':');
    // const hour = parseInt(hours, 10) + 5;
    // const minute = parseInt(minutes, 10) + 30;

    console.log("Hours is:", hours);
    console.log("Minutes is:",minutes);
    let min = parseInt(minutes) + 2
    return [hours,min]
    } else {
    return currentTime
  }
   }


  
}