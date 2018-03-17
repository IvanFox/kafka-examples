package me.ivanlis.streaming.example.favourite_color_app;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class FavColor {

    private final String name;
    private final Color color;

    enum Color {
        RED, GREEN, BLUE, BLACK, WHITE, GREY, BROWN, YELLOW, PURPLE
    }
}
