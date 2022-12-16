package com.wujunshen.entity.foodtruck;

import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2020/2/7 5:33 下午 <br>
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TimeRange {

	private Long id;
	private Date from;

	private Date to;
}
