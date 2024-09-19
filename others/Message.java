@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("messages")
public class Message {
    @Id
    private Long id;
    private String content;
}
