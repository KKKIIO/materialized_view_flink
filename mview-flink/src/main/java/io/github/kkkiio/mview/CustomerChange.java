package io.github.kkkiio.mview;

import lombok.Value;

@Value
public class CustomerChange {
    long id;
    String firstName;
    String lastName;
}