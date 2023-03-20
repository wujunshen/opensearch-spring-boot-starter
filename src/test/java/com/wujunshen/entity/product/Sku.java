package com.wujunshen.entity.product;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2020/2/7 17:33 <br>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Sku {

  private Long id;
  private String skuCode;
  private String skuName;
  private String color;
  private String size;
  private int skuPrice;
}
