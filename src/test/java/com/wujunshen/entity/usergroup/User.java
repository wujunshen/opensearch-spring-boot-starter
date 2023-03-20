package com.wujunshen.entity.usergroup;

import lombok.AllArgsConstructor;
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
public class User {

  private Long id;
  private String first;
  private String last;
}
