@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("messages")
public class Message {
    @Id
    private Long id;
    private String content;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("metadata")
public class Metadata {
    @Id
    private Long id;
    private Long messageId;
    private String details;
}
