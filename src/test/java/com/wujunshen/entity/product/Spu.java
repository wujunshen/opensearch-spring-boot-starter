package com.wujunshen.entity.product;

import java.util.ArrayList;
import java.util.List;
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
public class Spu {

	private Long id;

	private String productCode;

	private String productName;

	private String brandCode;

	private String brandName;

	private String categoryCode;

	private String categoryName;

	private String imageTag;

	private List<Sku> skus = new ArrayList<>();

	private String highlightedMessage;
}
